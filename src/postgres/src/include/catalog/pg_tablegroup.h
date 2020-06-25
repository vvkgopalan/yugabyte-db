/*-------------------------------------------------------------------------
 *
 * pg_tablegroup.h
 *	  definition of the "tablespace" system catalog (pg_tablegroup)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_tablegroup.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TABLEGROUP_H
#define PG_TABLEGROUP_H

#include "catalog/genbki.h"
#include "catalog/pg_tablegroup_d.h"

/* ----------------
 *		pg_tablegroup definition.  cpp turns this into
 *		typedef struct FormData_pg_tablegroup
 * ----------------
 */
CATALOG(pg_tablegroup,9000,TableGroupRelationId) BKI_ROWTYPE_OID(8999, TablegroupRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	NameData	grpname;		/* tablespace name */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		grpoptions[1];	/* per-tablespace options */
#endif
} FormData_pg_tablegroup;

/* ----------------
 *		Form_pg_tablegroup corresponds to a pointer to a tuple with
 *		the format of pg_tablegroup relation.
 * ----------------
 */
typedef FormData_pg_tablegroup *Form_pg_tablegroup;

#endif							/* PG_TABLEGROUP_H */
