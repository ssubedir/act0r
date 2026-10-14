use coding_task::add;

#[test]
fn adds_integers() {
    assert_eq!(add(2, 3), 5);
    assert_eq!(add(-2, 3), 1);
    assert_eq!(add(0, 4), 4);
}
