# Compaction in DuckLake

## The Problem

Every INSERT creates a new Parquet file. After thousands of inserts, you have thousands of tiny files. Queries slow down because opening each file has overhead - metadata parsing, network round-trips on cloud storage, memory allocation.

Compaction merges small files into larger ones. Fewer files, faster queries.

## Running Compaction

```sql
CALL ducklake_merge_adjacent_files('catalog', 'orders');
```

## The Challenge: Parallel Operations

Compaction doesn't run in isolation. While it's merging files, your application is:
- Inserting new orders
- Deleting cancelled orders
- Updating order statuses

How do these interact? Let's trace through each scenario.

---

## Compaction + Insert (Safe)

**Setup**: Table has files f1, f2, f3. Compaction wants to merge them. Application inserts new data.

```
Timeline:

T1  [Compaction] Starts transaction
    [Compaction] Reads snapshot 5
    [Compaction] Sees files: f1, f2, f3

T2  [Compaction] Reading f1, f2, f3 from disk...
    [Insert]     Starts transaction
    [Insert]     Reads snapshot 5

T3  [Compaction] Still reading...
    [Insert]     Writes new file f4 to disk
    [Insert]     Commits as snapshot 6
    [Insert]     Done ✓

T4  [Compaction] Finishes reading
    [Compaction] Writes f_merged to disk
    [Compaction] Tries to commit as snapshot 6
    [Compaction] FAILS - snapshot 6 already taken

T5  [Compaction] Automatic retry
    [Compaction] Reads new snapshot (now 6)
    [Compaction] Checks for conflicts: "Did anyone touch f1, f2, f3?"
    [Compaction] Answer: No, insert created f4 (different file)
    [Compaction] Commits as snapshot 7
    [Compaction] Done ✓
```

**Final state at snapshot 7**:
- f1, f2, f3: `end_snapshot = 7` (hidden)
- f_merged: `begin_snapshot = 7` (visible)
- f4: `begin_snapshot = 6` (visible)

**Why it works**: Insert and compaction touched different files. No logical conflict. The automatic retry handles the snapshot number collision.

---

## Compaction + Delete (Conflict)

**Setup**: Table has files f1, f2, f3. Compaction wants to merge them. Application deletes rows from f1.

```
Timeline:

T1  [Compaction] Starts transaction
    [Compaction] Reads snapshot 5
    [Compaction] Sees files: f1, f2, f3

T2  [Compaction] Reading f1... (contains row id=50)
    [Delete]     Starts transaction
    [Delete]     DELETE FROM orders WHERE id = 50
    [Delete]     This targets a row in f1

T3  [Compaction] Still reading f1, f2, f3...
    [Delete]     Writes delete marker: "f1, row 50 is deleted"
    [Delete]     Commits as snapshot 6
    [Delete]     Records: "deleted_from_table:orders"
    [Delete]     Done ✓

T4  [Compaction] Finishes reading (included row 50!)
    [Compaction] Writes f_merged (contains row 50!)
    [Compaction] Tries to commit as snapshot 6
    [Compaction] FAILS - snapshot 6 already taken

T5  [Compaction] Automatic retry
    [Compaction] Reads new snapshot (now 6)
    [Compaction] Checks for conflicts:
                 "What happened since snapshot 5?"
                 Answer: "deleted_from_table:orders"
    [Compaction] I'm compacting table 'orders'
    [Compaction] Someone deleted from 'orders'
    [Compaction] CONFLICT DETECTED
    [Compaction] ABORT with error
```

**Error message**:
```
Transaction conflict - attempting to compact table with index "1"
- but another transaction deleted from it
```

**Why it fails**: Compaction read row 50 and included it in f_merged. But row 50 was deleted. If we committed, queries would see row 50 (wrong). The conflict check catches this.

**What happens to f_merged on disk?** It gets cleaned up. The compaction transaction rolled back.

---

## Delete + Compaction (Conflict - Other Direction)

**Setup**: Same as above, but delete is slower and compaction commits first.

```
Timeline:

T1  [Delete]     Starts transaction
    [Delete]     Reads snapshot 5
    [Delete]     DELETE FROM orders WHERE id = 50

T2  [Delete]     Scanning f1 to find row 50...
    [Compaction] Starts transaction
    [Compaction] Reads snapshot 5
    [Compaction] Sees files: f1, f2, f3

T3  [Delete]     Still scanning...
    [Compaction] Reads f1, f2, f3 (fast, small files)
    [Compaction] Writes f_merged
    [Compaction] Commits as snapshot 6
    [Compaction] Records: "compacted_table:orders"
    [Compaction] Done ✓

T4  [Delete]     Found row 50 in f1
    [Delete]     Writes delete marker for f1
    [Delete]     Tries to commit as snapshot 6
    [Delete]     FAILS - snapshot 6 already taken

T5  [Delete]     Automatic retry
    [Delete]     Reads new snapshot (now 6)
    [Delete]     Checks for conflicts:
                 "What happened since snapshot 5?"
                 Answer: "compacted_table:orders"
    [Delete]     I'm deleting from table 'orders'
    [Delete]     Someone compacted 'orders'
    [Delete]     CONFLICT DETECTED
    [Delete]     ABORT with error
```

**Error message**:
```
Transaction conflict - attempting to delete from table with index "1"
- but another transaction has compacted it
```

**Why it fails**: Delete created a delete marker pointing to f1. But f1 no longer exists (replaced by f_merged). The delete marker would point to nothing, or worse, wrong data.

---

## Compaction + Update (Conflict)

Update = Delete + Insert. The delete part conflicts with compaction.

```
Timeline:

T1  [Compaction] Starts, reads snapshot 5, sees f1, f2, f3

T2  [Update]     UPDATE orders SET status = 'shipped' WHERE id = 50
    [Update]     This reads row 50 from f1
    [Update]     Writes delete marker for old row
    [Update]     Writes new file with updated row
    [Update]     Commits as snapshot 6
    [Update]     Records: "deleted_from_table:orders"

T3  [Compaction] Tries to commit
    [Compaction] Conflict: table was deleted from
    [Compaction] ABORT
```

Same conflict as delete. The compacted file contains the old version of row 50.

---

## Compaction + Compaction (Conflict)

Two compaction jobs targeting the same table.

```
Timeline:

T1  [Compaction A] Starts, reads snapshot 5, sees f1, f2, f3
    [Compaction B] Starts, reads snapshot 5, sees f1, f2, f3

T2  [Compaction A] Reads files, writes f_merged_a
    [Compaction B] Reads files, writes f_merged_b

T3  [Compaction A] Commits as snapshot 6
    [Compaction A] Records: "compacted_table:orders"
    [Compaction A] Done ✓

T4  [Compaction B] Tries to commit as snapshot 6
    [Compaction B] FAILS - already taken
    [Compaction B] Retry, checks conflicts
    [Compaction B] Sees: "compacted_table:orders"
    [Compaction B] I'm also compacting 'orders'
    [Compaction B] CONFLICT
    [Compaction B] ABORT
```

**Why**: Both read the same files. If both committed, we'd have f_merged_a and f_merged_b both containing f1+f2+f3 data. Duplicates.

---

## Insert + Insert (Safe)

Two inserts never conflict.

```
Timeline:

T1  [Insert A] Writes f4
    [Insert B] Writes f5

T2  [Insert A] Commits as snapshot 6
    [Insert B] Tries snapshot 6, fails, retries
    [Insert B] Checks conflicts: nothing relevant
    [Insert B] Commits as snapshot 7
```

Both succeed. They created different files.

---

## Delete + Delete on Same Row (Conflict)

```
Timeline:

T1  [Delete A] DELETE WHERE id = 50 (in f1)
    [Delete B] DELETE WHERE id = 50 (in f1)

T2  [Delete A] Writes delete marker for f1:row50
    [Delete A] Commits as snapshot 6

T3  [Delete B] Writes delete marker for f1:row50
    [Delete B] Tries to commit, retries
    [Delete B] Checks: "Did anyone delete from same files?"
    [Delete B] Sees: Delete A touched f1
    [Delete B] My delete marker also touches f1
    [Delete B] CONFLICT (both modified same file)
```

This prevents double-delete confusion.

---

## Delete + Delete on Different Rows (Safe)

```
Timeline:

T1  [Delete A] DELETE WHERE id = 50 (in f1)
    [Delete B] DELETE WHERE id = 999 (in f3)

T2  [Delete A] Writes delete marker for f1
    [Delete A] Commits as snapshot 6

T3  [Delete B] Writes delete marker for f3
    [Delete B] Retries, checks conflicts
    [Delete B] Delete A touched f1, I touch f3
    [Delete B] Different files, no conflict
    [Delete B] Commits as snapshot 7
```

---

## The Conflict Matrix

| First Commits | Second Tries | Same Table? | Same Files? | Result |
|---------------|--------------|-------------|-------------|--------|
| Insert | Insert | Yes | No (new files) | **Safe** |
| Insert | Delete | Yes | No | **Safe** |
| Insert | Compaction | Yes | No | **Safe** |
| Delete | Delete | Yes | Yes | **Conflict** |
| Delete | Delete | Yes | No | **Safe** |
| Delete | Compaction | Yes | Yes | **Conflict** |
| Compaction | Delete | Yes | Yes | **Conflict** |
| Compaction | Compaction | Yes | Yes | **Conflict** |
| Compaction | Insert | Yes | No | **Safe** |

Key insight: **Conflicts happen when operations touch the same files, not just the same table.**

Exception: Compaction is checked at table level because it potentially touches all files.

---

## The Retry Mechanism

When a transaction fails to commit (snapshot already taken), DuckLake automatically retries:

```
Attempt 1: Try commit snapshot N
           FAIL (someone else got N)

           Wait 100ms

Attempt 2: Get latest snapshot (now N)
           Check for conflicts
           If conflict → ABORT with error
           If clean → Try commit snapshot N+1
           FAIL (someone else got N+1)

           Wait 150ms (100 * 1.5)

Attempt 3: Get latest snapshot (now N+1)
           Check for conflicts
           Try commit snapshot N+2
           SUCCESS ✓
```

Configuration:
```sql
SET ducklake_max_retry_count = 10;   -- give up after 10 tries
SET ducklake_retry_wait_ms = 100;    -- initial wait
SET ducklake_retry_backoff = 1.5;    -- exponential backoff
```

---

## Compaction on Branches

On a branch, compaction sees:
1. Files created on this branch (owned)
2. Files inherited from parent branch (not owned)

When compacting:
- **Owned files**: Delete from metadata (we own them)
- **Inherited files**: Hide via `branch_file_deletion` table

```
Main branch:    f1, f2, f3 (created here)
Feature branch: f1, f2, f3 (inherited) + f4 (created here)

Compaction on feature branch:
- Reads: f1, f2, f3, f4
- Writes: f_merged
- For f1, f2, f3: INSERT INTO branch_file_deletion (hide from feature)
- For f4: DELETE FROM data_file (we own it)
- Result on feature: only f_merged visible
- Result on main: f1, f2, f3 still visible (unaffected)
```

---

## Why Conflicts Are Necessary

Consider if we allowed compaction + delete without conflict:

```
Compaction reads f1 (has row 50)
Delete commits (marks row 50 deleted in f1)
Compaction commits (f_merged contains row 50)

Query at latest snapshot:
- Sees f_merged (contains row 50)
- Delete marker points to f1 (no longer exists)
- Row 50 appears in results (WRONG - it was deleted)
```

The conflict check prevents this inconsistency.

---

## Best Practices

1. **Run compaction during quiet periods** - fewer conflicts with deletes/updates

2. **Don't run parallel compactions on same table** - they will conflict

3. **Expect conflicts during heavy write loads** - design your application to retry

4. **Use branches for isolation** - compaction on branch won't conflict with main

5. **Monitor conflict rates** - high conflicts mean compaction timing needs adjustment
