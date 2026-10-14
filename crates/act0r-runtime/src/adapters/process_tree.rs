//! Lifetime management for trusted local commands, separate from workflow durability.
use std::io;
use tokio::process::{Child, Command};

#[cfg(windows)]
mod platform {
    use super::*;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use windows_sys::Win32::{
        Foundation::INVALID_HANDLE_VALUE,
        System::{Diagnostics::ToolHelp::*, JobObjects::*, Threading::*},
    };

    pub struct ProcessTree(OwnedHandle);
    impl ProcessTree {
        pub fn configure(command: &mut Command) {
            // Assign the suspended process before any project code can spawn children.
            command.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
        }
        pub fn attach(child: &Child) -> io::Result<Self> {
            let pid = child
                .id()
                .ok_or_else(|| io::Error::other("missing process id"))?;
            // SAFETY: all handles are checked and owned exactly once. Structures are
            // zero-initialized C PODs, and pointers remain valid for each Win32 call.
            unsafe {
                let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if job.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let guard = Self(OwnedHandle::from_raw_handle(job));
                let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
                limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                if SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    std::mem::size_of_val(&limits) as u32,
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                let process = OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, pid);
                if process.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let process = OwnedHandle::from_raw_handle(process);
                if AssignProcessToJobObject(job, process.as_raw_handle()) == 0 {
                    return Err(io::Error::last_os_error());
                }
                let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
                if snapshot == INVALID_HANDLE_VALUE {
                    return Err(io::Error::last_os_error());
                }
                let snapshot = OwnedHandle::from_raw_handle(snapshot);
                let mut entry: THREADENTRY32 = std::mem::zeroed();
                entry.dwSize = std::mem::size_of_val(&entry) as u32;
                let mut available = Thread32First(snapshot.as_raw_handle(), &mut entry);
                while available != 0 {
                    if entry.th32OwnerProcessID == pid {
                        let thread = OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID);
                        if thread.is_null() {
                            return Err(io::Error::last_os_error());
                        }
                        let thread = OwnedHandle::from_raw_handle(thread);
                        if ResumeThread(thread.as_raw_handle()) == u32::MAX {
                            return Err(io::Error::last_os_error());
                        }
                        return Ok(guard);
                    }
                    available = Thread32Next(snapshot.as_raw_handle(), &mut entry);
                }
                Err(io::Error::other("suspended command thread was not found"))
            }
        }
        pub async fn terminate(&self) -> io::Result<()> {
            // SAFETY: the job handle is owned for this entire call. Termination applies
            // only to this command's job; no breakaway flags are enabled.
            if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                    unsafe { std::mem::zeroed() };
                // SAFETY: valid owned handle, initialized buffer of the declared size.
                if unsafe {
                    QueryInformationJobObject(
                        self.0.as_raw_handle(),
                        JobObjectBasicAccountingInformation,
                        (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                        std::mem::size_of_val(&info) as u32,
                        std::ptr::null_mut(),
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                if info.ActiveProcesses == 0 {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::other("command job did not terminate"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
    // Closing the last non-inherited job handle kills members, including on cancellation
    // of the enclosing future or abrupt host process exit. OwnedHandle supplies Drop.
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    pub struct ProcessTree {
        group: libc::pid_t,
        terminated: AtomicBool,
    }
    impl ProcessTree {
        pub fn configure(command: &mut Command) {
            command.process_group(0);
        }
        pub fn attach(child: &Child) -> io::Result<Self> {
            let pid = child
                .id()
                .ok_or_else(|| io::Error::other("missing process id"))?;
            Ok(Self {
                group: libc::pid_t::try_from(pid).map_err(io::Error::other)?,
                terminated: AtomicBool::new(false),
            })
        }
        fn kill(&self) -> io::Result<()> {
            // Never signal a previously terminated/reaped group again: its identifier
            // may have been recycled while the saved output was being collected.
            if self.terminated.swap(true, Ordering::Relaxed) {
                return Ok(());
            }
            // SAFETY: a positive child PID identifies its new process group. The negative
            // value targets that group, never the harness's own group.
            if unsafe { libc::kill(-self.group, libc::SIGKILL) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(())
            } else {
                self.terminated.store(false, Ordering::Relaxed);
                Err(error)
            }
        }
        pub async fn terminate(&self) -> io::Result<()> {
            self.kill()
        }
    }
    impl Drop for ProcessTree {
        fn drop(&mut self) {
            let _ = self.kill();
        }
    }
}

#[cfg(not(any(windows, unix)))]
mod platform {
    use super::*;
    pub struct ProcessTree;
    impl ProcessTree {
        pub fn configure(_: &mut Command) {}
        pub fn attach(_: &Child) -> io::Result<Self> {
            Err(io::Error::other(
                "process-tree execution is unsupported on this platform",
            ))
        }
        pub async fn terminate(&self) -> io::Result<()> {
            Ok(())
        }
    }
}
pub use platform::ProcessTree;
