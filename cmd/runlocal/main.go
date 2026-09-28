package runlocal

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/eugene-pi/simfleet/internal/envfile"
	"github.com/eugene-pi/simfleet/internal/execution"
	"github.com/eugene-pi/simfleet/internal/expander"
	"github.com/eugene-pi/simfleet/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		envfile.LoadEnv()
		dsn = os.Getenv("DATABASE_URL")
		if dsn == "" {
			log.Fatal("DATABASE_URL is not set")
		}
	}

	ctx := context.Background()

	st, err := store.New(ctx, dsn)
	must(err)
	defer st.Close()

	spec := mustLoadSpec(os.Args[1]) // YAML со спецификацией

	// 1. Создать эксперимент и развернуть перебор
	expID, n, err := expander.Create(ctx, st, spec)
	must(err)
	log.Printf("эксперимент %s: %d заданий", expID, n)

	// 2. Выполнить все задания последовательно
	exec := execution.New(st, blob.NewLocalFS("./out"), runner.Registry{
		"crossing": crossing.New(),
	}, "local-1")

	for {
		ids, err := st.NextQueued(ctx, expID, 100)
		must(err)
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if err := exec.Execute(ctx, id); err != nil {
				log.Printf("задание %s: %v", id, err)
			}
		}
	}

	// 3. Сводка
	sum, err := st.Summary(ctx, expID)
	must(err)
	fmt.Printf("выполнено %d, отказов %d\n", sum.Completed, sum.Failed)
}
