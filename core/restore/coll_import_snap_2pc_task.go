package restore

import "context"

// snapshotImport2PCTask holds a snapshot import at Uncommitted until every
// secondary's mirrored job is ready. The primary commit broadcasts the fence
// that commits those mirrors; no import lifecycle call is made on a secondary.
type snapshotImport2PCTask struct {
	importer    *snapshotImporter
	secondaries []importSecondary
}

func (it *snapshotImport2PCTask) Execute(ctx context.Context) error {
	if err := it.execute(ctx); err != nil {
		it.importer.job.SetFailed(err)
		return err
	}

	return nil
}

func (it *snapshotImport2PCTask) execute(ctx context.Context) error {
	jobID, err := it.importer.submit2PC(ctx)
	if err != nil {
		return err
	}

	// Before the primary commit, aborting it replicates a rollback that fails
	// the mirrors. Afterwards rollback is impossible and a mirror failure is
	// reported as divergence instead.
	committed := false
	defer func() {
		if !committed {
			it.importer.abort(context.WithoutCancel(ctx), jobID)
		}
	}()

	if err := it.importer.waitUncommitted(ctx, jobID); err != nil {
		return err
	}
	for _, secondary := range it.secondaries {
		if err := waitSecondaryMirrored(ctx, it.importer.logger, it.importer.target.DBName(), secondary, jobID); err != nil {
			return err
		}
	}

	if err := it.importer.commit(ctx, jobID); err != nil {
		return err
	}
	committed = true

	for _, secondary := range it.secondaries {
		if err := waitSecondaryCommitted(ctx, it.importer.logger, it.importer.target.DBName(), secondary, jobID); err != nil {
			return err
		}
	}
	return it.importer.waitCompleted(ctx, jobID)
}
