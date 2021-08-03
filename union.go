package dbr

import (
	"fmt"
	"github.com/gocraft/dbr/v2/dialect"
	"strings"
)

type orderBy struct {
	col string
	asc bool
}

type union struct {
	builder []Builder
	all     bool
	orderBy []orderBy
	limit uint
}

// Union builds `... UNION ...`.
func Union(builder ...Builder) *union {
	return &union{
		builder: builder,
		orderBy: make([]orderBy, 0),
	}
}

// UnionAll builds `... UNION ALL ...`.
func UnionAll(builder ...Builder) *union {
	return &union{
		builder: builder,
		all:     true,
		orderBy: make([]orderBy, 0),
	}
}

func (u *union) OrderBy(col string, asc bool) *union {
	u.orderBy = append(u.orderBy, orderBy{
		col: col,
		asc: asc,
	})
	return u
}

func (u *union) Limit(limit uint) *union {
	u.limit = limit
	return u
}

func (u *union) Build(d Dialect, buf Buffer) error {
	for i, b := range u.builder {
		if i > 0 {
			buf.WriteString(" UNION ")
			if u.all {
				buf.WriteString("ALL ")
			}
		}
		if d == dialect.MySQL {
			buf.WriteString("( ")
		}
		err := b.Build(d, buf)
		if d == dialect.MySQL {
			buf.WriteString(" )")
		}
		if err != nil {
			return err
		}
	}
	if len(u.orderBy) > 0 {
		buf.WriteString(" ORDER BY ")
		orderBys := make([]string, len(u.orderBy))
		for i, order := range u.orderBy {
			orderBys[i] =
				fmt.Sprintf("%s %s", order.col, func () string {
					if order.asc {
						return "ASC"
					} else {
						return "DESC"
					}
				}())
		}
		buf.WriteString(strings.Join(orderBys, ", "))
	}

	if u.limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", u.limit))
	}
	return nil
}

func (u *union) As(alias string) Builder {
	return as(u, alias)
}
