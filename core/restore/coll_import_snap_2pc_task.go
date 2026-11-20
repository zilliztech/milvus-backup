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
	si := it.importer
	rt := importViaRESTFul2PCTask{
		importer: &restfulImporter{
			args: importTaskArgs{
				target:     si.target,
				restfulCli: si.restfulCli,
				logger:     si.logger,
				jobs:       si.jobs,
			},
			job: si.job,
		},
		secondaries: it.secondaries,
	}
	return rt.executeImport(ctx, si.submit2PC)
}
