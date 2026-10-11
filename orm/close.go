package orm

// closeRows releases a read query's rows; the error is irrelevant after a
// completed read, unlike writes whose results are checked.
func closeRows(rows *resultRows) {
	_ = rows.Close()
}
