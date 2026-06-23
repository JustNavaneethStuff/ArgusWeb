from argus_core.jobs import compute_job_progress, list_jobs


def test_list_jobs_return_shape():
    assert callable(list_jobs)
    assert callable(compute_job_progress)
