// internal/expander/seed.go
package expander

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/eugene-pi/simfleet/internal/core"
)

// DeriveSeed вычисляет зерно детерминированно из идентификатора
// эксперимента и номера сочетания. Одинаковый вход всегда даёт
// одинаковое зерно — это основа воспроизводимости прогонов.
func DeriveSeed(expID core.ExperimentID, idx int) int64 {
	h := fnv.New64a()
	b := [16]byte(expID)
	h.Write(b[:])

	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(idx))
	h.Write(n[:])

	return int64(h.Sum64() &^ (1 << 63)) // сбросить знаковый бит: зерно неотрицательное
}
