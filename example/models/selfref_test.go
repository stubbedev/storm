package models_test

import (
	"testing"

	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

// Employee, Department and Badge give through relations that pass back
// through the parent's own table: an employee's reports through the
// departments they manage (the related table is the parent's), and the
// badges of an employee's mentees (the intermediate table is).
type Employee struct {
	orm.Model
	ID           int64  `db:"id"`
	Name         string `db:"name"`
	DepartmentID int64  `db:"department_id,nullzero"`
	MentorID     int64  `db:"mentor_id,nullzero"`
}

type Department struct {
	orm.Model
	ID        int64 `db:"id"`
	ManagerID int64 `db:"manager_id"`
}

type Badge struct {
	orm.Model
	ID         int64  `db:"id"`
	EmployeeID int64  `db:"employee_id"`
	Name       string `db:"name"`
}

func modelTable[M any](name string, cols []string, ptr func(*M, string) any, state func(*M) *orm.Model) *orm.Table[M] {
	return &orm.Table[M]{Name: name, PrimaryKey: "id", KeyType: orm.KeyManual, Columns: cols, Ptr: ptr, State: state}
}

var (
	employeeTable = modelTable("employees", []string{"id", "name", "department_id", "mentor_id"}, func(m *Employee, c string) any {
		return map[string]any{"id": &m.ID, "name": &m.Name, "department_id": &m.DepartmentID, "mentor_id": &m.MentorID}[c]
	}, func(m *Employee) *orm.Model { return &m.Model })
	departmentTable = modelTable("departments", []string{"id", "manager_id"}, func(m *Department, c string) any {
		return map[string]any{"id": &m.ID, "manager_id": &m.ManagerID}[c]
	}, func(m *Department) *orm.Model { return &m.Model })
	badgeTable = modelTable("badges", []string{"id", "employee_id", "name"}, func(m *Badge, c string) any {
		return map[string]any{"id": &m.ID, "employee_id": &m.EmployeeID, "name": &m.Name}[c]
	}, func(m *Badge) *orm.Model { return &m.Model })

	employeeID       = orm.NewColumn(employeeTable, "id", func(m *Employee) *int64 { return &m.ID })
	employeeName     = orm.NewColumn(employeeTable, "name", func(m *Employee) *string { return &m.Name })
	employeeDept     = orm.NewColumn(employeeTable, "department_id", func(m *Employee) *int64 { return &m.DepartmentID })
	employeeMentor   = orm.NewColumn(employeeTable, "mentor_id", func(m *Employee) *int64 { return &m.MentorID })
	departmentID     = orm.NewColumn(departmentTable, "id", func(m *Department) *int64 { return &m.ID })
	departmentLeader = orm.NewColumn(departmentTable, "manager_id", func(m *Department) *int64 { return &m.ManagerID })
	badgeOwner       = orm.NewColumn(badgeTable, "employee_id", func(m *Badge) *int64 { return &m.EmployeeID })

	managedEmployees = orm.HasManyThrough(employeeID, departmentLeader, departmentID, employeeDept, func(*Employee, []Employee) {})
	menteeBadges     = orm.HasManyThrough(employeeID, employeeMentor, employeeID, badgeOwner, func(*Employee, []Badge) {})
)

func init() { employeeTable.NullZero = []string{"department_id", "mentor_id"} }

func TestSelfReferentialThrough(t *testing.T) {
	ctx := setup(t)
	s := schema.On(orm.DefaultConnection)
	for _, table := range []string{"badges", "departments", "employees"} {
		tCheck(t, s.DropIfExists(ctx, table))
	}
	tCheck(t, s.Create(ctx, "employees", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.String("name")
		t.BigInteger("department_id").Nullable()
		t.BigInteger("mentor_id").Nullable()
	}))
	tCheck(t, s.Create(ctx, "departments", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.BigInteger("manager_id")
	}))
	tCheck(t, s.Create(ctx, "badges", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.BigInteger("employee_id")
		t.String("name")
	}))
	t.Cleanup(func() {
		for _, table := range []string{"badges", "departments", "employees"} {
			tCheck(t, s.DropIfExists(ctx, table))
		}
	})
	ok(employeeTable.Query().Insert(ctx,
		Employee{ID: 1, Name: "Ann"},
		Employee{ID: 2, Name: "Bob", DepartmentID: 1, MentorID: 1},
		Employee{ID: 3, Name: "Cy", DepartmentID: 1, MentorID: 2},
		Employee{ID: 4, Name: "Dee"}))
	ok(departmentTable.Query().Insert(ctx, Department{ID: 1, ManagerID: 1}))
	ok(badgeTable.Query().Insert(ctx, Badge{ID: 1, EmployeeID: 2, Name: "gold"}, Badge{ID: 2, EmployeeID: 3, Name: "silver"}))

	names := func(q orm.Query[Employee]) []string {
		return must(q.OrderBy(employeeID.Asc()).Pluck(ctx, employeeName))
	}
	eq(t, "whereHas through the parent's own table as related", names(employeeTable.Query().WhereHas(managedEmployees)), []string{"Ann"})
	eq(t, "hasCount through the parent's own table", names(employeeTable.Query().HasCount(managedEmployees, orm.Gte, 2)), []string{"Ann"})
	eq(t, "whereHas through the parent's own table as intermediate", names(employeeTable.Query().WhereHas(menteeBadges)), []string{"Ann", "Bob"})
	eq(t, "whereDoesntHave through it", names(employeeTable.Query().WhereDoesntHave(menteeBadges)), []string{"Cy", "Dee"})
}
