package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"strings"

	"github.com/4chain-ag/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

type SQLiteEventLookup struct {
	wdb            *sql.DB
	rdb            *sql.DB
	insEvent       *sql.Stmt
	updSpend       *sql.Stmt
	delOutpoint    *sql.Stmt
	updBlockHeight *sql.Stmt
	storage        engine.Storage
}

func NewSQLiteEventLookup(storage engine.Storage, dbPath string) *SQLiteEventLookup {
	var err error
	l := &SQLiteEventLookup{
		storage: storage,
	}
	if l.wdb, err = sql.Open("sqlite3", dbPath); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec("PRAGMA temp_store=MEMORY;"); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec("PRAGMA mmap_size=30000000000;"); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec(`CREATE TABLE IF NOT EXISTS events(
		event TEXT, 
		outpoint TEXT,
		height INTEGER,
		idx BIGINT,
		spent BOOLEAN NOT NULL DEFAULT FALSE,
		PRIMARY KEY(outpoint, event)
	)`); err != nil {
		log.Panic(err)
	} else if _, err = l.wdb.Exec(`CREATE INDEX IF NOT EXISTS idx_events_event ON events(event, height, idx)`); err != nil {
		log.Panic(err)
	} else if l.insEvent, err = l.wdb.Prepare(`INSERT INTO events(event, outpoint, height, idx) 
		VALUES(?1, ?2, ?3, ?4)
		ON CONFLICT DO UPDATE  SET height = ?3, idx = ?4`,
	); err != nil {
		log.Panic(err)
	} else if l.updSpend, err = l.wdb.Prepare(`UPDATE events SET spent = TRUE WHERE outpoint = ?`); err != nil {
		log.Panic(err)
	} else if l.delOutpoint, err = l.wdb.Prepare(`DELETE FROM events WHERE outpoint = ?`); err != nil {
		log.Panic(err)
	} else if l.updBlockHeight, err = l.wdb.Prepare(`UPDATE events 
			SET height=?2, idx=?3
			WHERE outpoint=?1`); err != nil {
		log.Panic(err)
	}
	l.wdb.SetMaxOpenConns(1)

	if l.rdb, err = sql.Open("sqlite3", dbPath); err != nil {
		log.Panic(err)
	} else if _, err = l.rdb.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		log.Panic(err)
	} else if _, err = l.rdb.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		log.Panic(err)
	} else if _, err = l.rdb.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		log.Panic(err)
	} else if _, err = l.rdb.Exec("PRAGMA temp_store=MEMORY;"); err != nil {
		log.Panic(err)
	} else if _, err = l.rdb.Exec("PRAGMA mmap_size=30000000000;"); err != nil {
		log.Panic(err)
	}
	return l
}

func (l *SQLiteEventLookup) SaveEvent(outpoint *overlay.Outpoint, event string, height uint32, idx uint64) error {
	_, err := l.insEvent.Exec(
		event,
		outpoint.String(),
		height,
		idx,
	)
	return err
}
func (l *SQLiteEventLookup) SaveEvents(outpoint *overlay.Outpoint, events []string, height uint32, idx uint64) error {
	for _, event := range events {
		if _, err := l.insEvent.Exec(
			event,
			outpoint.String(),
			height,
			idx,
		); err != nil {
			return err
		}
	}
	return nil
}
func (l *SQLiteEventLookup) Close() {
	if l.wdb != nil {
		l.wdb.Close()
	}
	if l.insEvent != nil {
		l.insEvent.Close()
	}
	if l.updSpend != nil {
		l.updSpend.Close()
	}
	if l.delOutpoint != nil {
		l.delOutpoint.Close()
	}
	if l.updBlockHeight != nil {
		l.updBlockHeight.Close()
	}
}

func (l *SQLiteEventLookup) Lookup(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
	var sql strings.Builder
	question := &Question{}
	if err := json.Unmarshal(q.Query, question); err != nil {
		return nil, err
	}
	args := []interface{}{question.Event, question.From.Height, question.From.Idx}
	sql.WriteString(`SELECT outpoint FROM events 
		WHERE event = ?1 AND (height >= ?2 OR (height=?2 AND idx >= ?3))`)
	if question.Spent != nil {
		sql.WriteString(" AND spent = ?")
		args = append(args, *question.Spent)
	}
	if question.Reverse {
		sql.WriteString(" ORDER BY event, height DESC, idx DESC")
	} else {
		sql.WriteString(" ORDER BY event, height, idx")
	}
	if question.Limit > 0 {
		sql.WriteString(" LIMIT ?")
		args = append(args, question.Limit)
	}
	if rows, err := l.rdb.Query(sql.String(), args...); err != nil {
		return nil, err
	} else {
		defer rows.Close()
		answer := &lookup.LookupAnswer{
			Type: lookup.AnswerTypeOutputList,
		}
		for rows.Next() {
			var op string
			if err := rows.Scan(&op); err != nil {
				return nil, err
			}
			if outpoint, err := overlay.NewOutpointFromString(op); err != nil {
				return nil, err
			} else if outpoint != nil {
				if output, err := l.storage.FindOutput(ctx, outpoint, nil, nil, true); err != nil {
					return nil, err
				} else if output != nil {
					if beef, _, _, err := transaction.ParseBeef(output.Beef); err != nil {
						return nil, err
					} else {
						if output.AncillaryBeef != nil {
							if err = beef.MergeBeefBytes(output.AncillaryBeef); err != nil {
								return nil, err
							}
						}
						if beefBytes, err := beef.AtomicBytes(&outpoint.Txid); err != nil {
							return nil, err
						} else {
							answer.Outputs = append(answer.Outputs, &lookup.OutputListItem{
								OutputIndex: output.Outpoint.OutputIndex,
								Beef:        beefBytes,
							})
						}
					}
				}
			}
		}
		return answer, nil
	}

}

func (l *SQLiteEventLookup) OutputSpent(ctx context.Context, outpoint *overlay.Outpoint, _ string) error {
	if _, err := l.updSpend.Exec(outpoint.String()); err != nil {
		return err
	}
	return nil
}

func (l *SQLiteEventLookup) OutputsSpent(ctx context.Context, outpoints []*overlay.Outpoint, _ string) error {
	args := []interface{}{}
	for _, outpoint := range outpoints {
		args = append(args, outpoint.String())
	}
	_, err := l.wdb.ExecContext(ctx,
		`UPDATE events SET spent = TRUE WHERE outpoint IN (`+placeholders(len(outpoints))+`)`,
		args...,
	)
	return err
}

func (l *SQLiteEventLookup) OutputDeleted(ctx context.Context, outpoint *overlay.Outpoint, topic string) error {
	if _, err := l.delOutpoint.Exec(outpoint.String()); err != nil {
		return err
	}
	return nil
}

func (l *SQLiteEventLookup) OutputBlockHeightUpdated(ctx context.Context, outpoint *overlay.Outpoint, blockHeight uint32, blockIdx uint64) error {
	if _, err := l.updBlockHeight.Exec(outpoint.String(), blockHeight, blockIdx); err != nil {
		return err
	}
	return nil
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return "?" + strings.Repeat(",?", n-1)
}
