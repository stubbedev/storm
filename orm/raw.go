package orm

import "context"

// Select runs a raw query on the default connection and returns its rows
// as maps (DB::select). Placeholders are ? on every engine; the arguments
// are bound, never interpolated.
func Select(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	return SelectOn(ctx, DefaultConnection, query, args...)
}

// SelectOn is Select on a named connection.
func SelectOn(ctx context.Context, conn, query string, args ...any) ([]map[string]any, error) {
	rows, err := readRows(ctx, conn, func(b *SQL) { b.Raw(query, args...) })
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	return scanMaps(rows)
}

// Statement runs a raw statement on the default connection and reports
// how many rows it affected (DB::statement, DB::insert, DB::update,
// DB::delete). Placeholders are ? on every engine.
func Statement(ctx context.Context, query string, args ...any) (int64, error) {
	return StatementOn(ctx, DefaultConnection, query, args...)
}

// StatementOn is Statement on a named connection.
func StatementOn(ctx context.Context, conn, query string, args ...any) (int64, error) {
	res, err := execOn(ctx, conn, func(b *SQL) { b.Raw(query, args...) })
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
