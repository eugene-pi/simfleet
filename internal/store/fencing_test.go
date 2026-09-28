package store

func TestFencing(t *testing.T) {
	pool := newStore(t)
	jobID := seedQueuedJob(t, pool)

	// исполнитель A берёт задание
	attemptA, err := pool.Claim(ctx, jobID, "worker-a")
	require.NoError(t, err)

	// аренда истекла, контроллер вернул задание в очередь
	expireLease(t, jobID)
	expired, err := pool.ReclaimExpired(ctx, 10)
	require.Len(t, expired, 1)

	// исполнитель B берёт его заново
	attemptB, err := pool.Claim(ctx, jobID, "worker-b")
	require.NoError(t, err)
	require.Equal(t, attemptA+1, attemptB)

	// A «просыпается» и пытается записать результат со старым номером попытки
	err = pool.SaveResult(ctx, Result{JobID: jobID, Attempt: attemptA, ...})
	require.ErrorIs(t, err, ErrFenced)

	// B записывает успешно
	require.NoError(t, pool.SaveResult(ctx, Result{JobID: jobID, Attempt: attemptB, ...}))
}

func TestConcurrentClaim(t *testing.T) {
	// 20 горутин пытаются взять одно задание;
	// ровно одна получает номер попытки, остальные — ErrNotClaimable
}