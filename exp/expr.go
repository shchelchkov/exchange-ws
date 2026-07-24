package exp

import (
	"fmt"

	"github.com/expr-lang/expr"
)

type Env struct {
	Value Value
}
type Program struct {
}

func (p *Program) contains(arr []interface{}, val interface{}) bool {
	for _, x := range arr {
		if x == val {
			return true
		}
	}
	return false
}

type Value struct {
	Int int
}

func main() {

	env := map[string]interface{}{
		"order": map[string]interface{}{
			"total": 1300,
		},

		"customer": map[string]interface{}{
			"vip": true,
		},
		"tags": []interface{}{"1", "2"},
	}

	exprStr := `order.total > 1000 && customer.vip && contains(tags, "1")`

	fn := expr.Function("contains",
		func(params ...any) (any, error) {

			arr := params[0].([]interface{})
			val := params[1].(interface{})

			for _, x := range arr {
				if x == val {
					return true, nil
				}
			}
			return false, nil
		},
		new(func(arr []interface{}, val interface{}) bool),
	)

	op := expr.Operator("contains", "contains")
	//p := &Program{}

	opt := []expr.Option{
		expr.Env(env),
		op,
		fn,
	}
	program, err := expr.Compile(exprStr, opt...)

	if err != nil {
		panic(err)
	}

	output, err := expr.Run(program, env)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Rule evaluation: %v\n", output)

}
