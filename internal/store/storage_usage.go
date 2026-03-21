package store

import "sort"

type StorageUsage struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ProjectID   string `json:"projectId,omitempty"`
	ProjectName string `json:"projectName,omitempty"`
	Bytes       int64  `json:"bytes"`
}

type StorageUsageSnapshot struct {
	TopProjects []StorageUsage `json:"topProjects"`
	TopRuns     []StorageUsage `json:"topRuns"`
}

func (db *DB) StorageUsageSnapshot() StorageUsageSnapshot {
	topProjects, topRuns := db.topStorageUsage()
	return StorageUsageSnapshot{
		TopProjects: topProjects,
		TopRuns:     topRuns,
	}
}

func (db *DB) topStorageUsage() ([]StorageUsage, []StorageUsage) {
	rows, err := db.Query(`
		SELECT
			r.id,
			r.project_id,
			p.name,
			COALESCE(NULLIF(r.display_name, ''), r.name),
			(
				LENGTH(COALESCE(r.id, '')) +
				LENGTH(COALESCE(r.project_id, '')) +
				LENGTH(COALESCE(r.name, '')) +
				LENGTH(COALESCE(r.display_name, '')) +
				LENGTH(COALESCE(r.config, '')) +
				LENGTH(COALESCE(r.summary, '')) +
				LENGTH(COALESCE(r.state, '')) +
				LENGTH(COALESCE(r.host, '')) +
				LENGTH(COALESCE(r.program, '')) +
				LENGTH(COALESCE(r.git_commit, '')) +
				LENGTH(COALESCE(r.tags, '')) +
				LENGTH(COALESCE(r.notes, '')) +
				LENGTH(COALESCE(r.group_name, '')) +
				LENGTH(COALESCE(r.job_type, '')) +
				LENGTH(COALESCE(r.sweep_name, '')) +
				64 +
				COALESCE(hs.bytes, 0) +
				COALESCE(hh.bytes, 0) +
				COALESCE(rs.bytes, 0) +
				COALESCE(se.bytes, 0) +
				COALESCE(cl.bytes, 0) +
				COALESCE(a.bytes, 0) +
				COALESCE(f.bytes, 0) +
				COALESCE(rk.bytes, 0)
			) AS total_bytes
		FROM runs r
		JOIN projects p ON p.id = r.project_id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(key, '')) + 40) AS bytes
			FROM history_scalars
			GROUP BY run_id
		) hs ON hs.run_id = r.id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(key, '')) + LENGTH(COALESCE(bins, '')) + LENGTH(COALESCE(vals, '')) + 48) AS bytes
			FROM history_histograms
			GROUP BY run_id
		) hh ON hh.run_id = r.id
		LEFT JOIN (
			SELECT run_id, COUNT(*) * 24 AS bytes
			FROM run_steps
			GROUP BY run_id
		) rs ON rs.run_id = r.id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(data, '')) + 24) AS bytes
			FROM system_events
			GROUP BY run_id
		) se ON se.run_id = r.id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(line, '')) + LENGTH(COALESCE(stream, '')) + 24) AS bytes
			FROM console_logs
			GROUP BY run_id
		) cl ON cl.run_id = r.id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(id, '')) + LENGTH(COALESCE(type, '')) + LENGTH(COALESCE(name, '')) + LENGTH(COALESCE(digest, '')) + LENGTH(COALESCE(state, '')) + LENGTH(COALESCE(metadata, '')) + 24) AS bytes
			FROM artifacts
			GROUP BY run_id
		) a ON a.run_id = r.id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(id, '')) + LENGTH(COALESCE(name, '')) + LENGTH(COALESCE(url, '')) + 16) AS bytes
			FROM files
			GROUP BY run_id
		) f ON f.run_id = r.id
		LEFT JOIN (
			SELECT run_id, SUM(LENGTH(COALESCE(key, '')) + 16) AS bytes
			FROM run_keys
			GROUP BY run_id
		) rk ON rk.run_id = r.id
		WHERE r.deleted_at IS NULL AND p.deleted_at IS NULL
	`)
	if err != nil {
		return nil, nil
	}
	defer rows.Close()
	snapshot := storageUsageSnapshotFromRows(rows)
	return snapshot.TopProjects, snapshot.TopRuns
}

func storageUsageSnapshotFromRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) StorageUsageSnapshot {
	var runs []StorageUsage
	projectsByID := map[string]StorageUsage{}
	for rows.Next() {
		var item StorageUsage
		if err := rows.Scan(&item.ID, &item.ProjectID, &item.ProjectName, &item.Name, &item.Bytes); err != nil {
			return StorageUsageSnapshot{}
		}
		runs = append(runs, item)
		project := projectsByID[item.ProjectID]
		project.ID = item.ProjectID
		project.Name = item.ProjectName
		project.Bytes += item.Bytes
		projectsByID[item.ProjectID] = project
	}
	if err := rows.Err(); err != nil {
		return StorageUsageSnapshot{}
	}

	projects := make([]StorageUsage, 0, len(projectsByID))
	for _, project := range projectsByID {
		projects = append(projects, project)
	}

	sort.Slice(runs, func(i, j int) bool {
		if runs[i].Bytes == runs[j].Bytes {
			if runs[i].ProjectName == runs[j].ProjectName {
				return runs[i].Name < runs[j].Name
			}
			return runs[i].ProjectName < runs[j].ProjectName
		}
		return runs[i].Bytes > runs[j].Bytes
	})
	sort.Slice(projects, func(i, j int) bool {
		if projects[i].Bytes == projects[j].Bytes {
			return projects[i].Name < projects[j].Name
		}
		return projects[i].Bytes > projects[j].Bytes
	})

	if len(runs) > 3 {
		runs = runs[:3]
	}
	if len(projects) > 3 {
		projects = projects[:3]
	}
	return StorageUsageSnapshot{
		TopProjects: projects,
		TopRuns:     runs,
	}
}
