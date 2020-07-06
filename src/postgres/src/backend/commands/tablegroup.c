// tablegroup.c
//	  Commands to manipulate table groups
// TODO: add description
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#include "postgres.h"

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablegroup.h"
#include "commands/comment.h"
#include "commands/seclabel.h"
#include "commands/tablecmds.h"
#include "commands/tablegroup.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/standby.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/varlena.h"

/*
 * Create a table group
 */
Oid
CreateTableGroup(CreateTableGroupStmt *stmt)
{
	Relation	rel;
	Datum		values[Natts_pg_tablegroup];
	bool		nulls[Natts_pg_tablegroup];
	HeapTuple	tuple;
	Oid			tablegroupoid;
	Oid 		ownerId;

	if (MyDatabaseColocated)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use tablegroups in a colocated database")));

	/*
	 * Check that there is no other tablegroup by this name.
	 */
	if (OidIsValid(get_tablegroup_oid(stmt->tablegroupname, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("tablegroup \"%s\" already exists",
						stmt->tablegroupname)));

	if (stmt->owner)
		ownerId = get_rolespec_oid(stmt->owner, false);
	else
		ownerId = GetUserId();

	/*
	 * Insert tuple into pg_tablegroup.  The purpose of doing this first is to
	 * lock the proposed tablegroupname against other would-be creators. The
	 * insertion will roll back if we find problems below.
	 */
	rel = heap_open(TableGroupRelationId, RowExclusiveLock);

	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pg_tablegroup_grpname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->tablegroupname));

	/* Generate new proposed grpoptions (text array) */
	/* For now no grpoptions. Will be part of Interleaved/Copartitioned */
	
	nulls[Anum_pg_tablegroup_grpoptions - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	tablegroupoid = CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	/* We keep the lock on pg_tablegroup until commit */
	heap_close(rel, NoLock);

	return tablegroupoid;
}

/*
 * Drop a tablegroup
 *
 * Permissions? Must be owner?
 */
void
DropTableGroup(DropTableGroupStmt *stmt)
{
	char	   *tablegroupname = stmt->tablegroupname;
	HeapScanDesc scandesc;
	SysScanDesc class_scandesc;
	Relation	rel;
	Relation 	class_rel;
	HeapTuple	tuple;
	HeapTuple 	class_tuple;
	ScanKeyData entry[1];
	ScanKeyData class_entry[1];
	Oid			tablegroupoid;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(TableGroupRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablegroup_grpname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(tablegroupname));
	scandesc = heap_beginscan_catalog(rel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablegroup \"%s\" does not exist",
						tablegroupname)));
		return;
	}

	tablegroupoid = HeapTupleGetOid(tuple);

	// Scan uses pg_class_tblgrp_index since pg_class can grow large
	class_rel = heap_open(RelationRelationId, RowExclusiveLock);
	ScanKeyInit(&class_entry[0],
				Anum_pg_class_reltablegroup,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tablegroupoid));
	class_scandesc = systable_beginscan(class_rel,
									    ClassTblgrpIndexId,
									    true,
									    NULL,
									    1,
									    class_entry);
	class_tuple = systable_getnext(class_scandesc);
	systable_endscan(class_scandesc);
	heap_close(class_rel, NoLock);
	if (HeapTupleIsValid(class_tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("tablegroup \"%s\" is not empty",
						tablegroupname)));
		heap_endscan(scandesc);
		heap_close(rel, NoLock);
		return;
	}

	/* DROP hook for the tablegroup being removed */
	InvokeObjectDropHook(TableGroupRelationId, tablegroupoid, 0);

	/*
	 * Remove the pg_tablegroup tuple
	 */
	CatalogTupleDelete(rel, tuple);

	heap_endscan(scandesc);

	/* We keep the lock on pg_tablegroup until commit */
	heap_close(rel, NoLock);
}

/*
 * get_tablegroup_oid - given a tablegroup name, look up the OID
 *
 * If missing_ok is false, throw an error if tablegroup name not found.  If
 * true, just return InvalidOid.
 */
Oid
get_tablegroup_oid(const char *tablegroupname, bool missing_ok)
{
	Oid			result;
	Relation	rel;
	HeapScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	/*
	 * Search pg_tablegroup.  We use a heapscan here even though there is an
	 * index on name, on the theory that pg_tablegroup will usually have just
	 * a few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(TableGroupRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablegroup_grpname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(tablegroupname));
	scandesc = heap_beginscan_catalog(rel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = HeapTupleGetOid(tuple);
	else
		result = InvalidOid;

	heap_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	if (!OidIsValid(result) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablegroup \"%s\" does not exist",
						tablegroupname)));

	return result;
}

/*
 * get_tablegroup_name - given a tablegroup OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such tablegroup.
 */
char *
get_tablegroup_name(Oid grp_oid)
{
	char	   *result;
	Relation	rel;
	HeapScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	/*
	 * Search pg_tablegroup.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_tablegroup will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(TableGroupRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(grp_oid));
	scandesc = heap_beginscan_catalog(rel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = pstrdup(NameStr(((Form_pg_tablegroup) GETSTRUCT(tuple))->grpname));
	else
		result = NULL;

	heap_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return result;
}

