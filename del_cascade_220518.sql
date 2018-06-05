DROP TYPE IF EXISTS tp_delete_cascade CASCADE;
--DROP FUNCTION IF EXISTS service.del_cascade_support(text, text, bigint);
create type tp_delete_cascade	as
(
	r_n		bigint, --row number
	table_fk	text,   --foreign key table name 
	allias_fk	text,   --allias for foreign key table name
	table_pk	text,   --primary key table name
	allias_pk	text,   --allias for primary key table name
	pk		text,   --primary key column name
	fk		text,   --foreign key column name
	conname		text,   --constraint name
	values_fk	text    --values)
);


CREATE OR REPLACE FUNCTION del_cascade_support(_table text, _col text, _id bigint)
returns	setof tp_delete_cascade	as
$body$
declare

--support part
	T_sql_sup	text;	--main query
	T_sql_col_list	text;	--column list for select
	T_sql_join_list	text;	--left join list
	T_allias_mt 	text;	--allias for main table
--executable part
	T_sql_head	text;	--contains: with recursive, from
	T_sql_body	text;	--contains: left join, where, group by, order by

begin

--------------
--Check part--
--------------
	----------------------------
	--All arg must be not null--
	----------------------------
	if _table is null
		then	raise EXcEPTION '_table must be NOT NULL';
	elsif 	_col	is null
		then	raise EXcEPTION '_col must be NOT NULL';
	elsif 	_id	is null
		then	raise EXcEPTION '_id must be NOT NULL';
	end if;
	

----------------
--Support part--
----------------
T_sql_sup := 
$$
	with	sql_main	as	
	(
		select	
			table_fk,
			allias_fk,
			table_pk,
			allias_pk,
			pk,
			fk,
			conname					

			from	(
					select
						pc.conrelid::regclass::text 	table_fk, 
						pc.confrelid::regclass::text	table_pk,
						pa_pk.attname	pk,
						pc.conname,

--! поменять на pcl_fk.relnamespace::regnamespace::text||pcl_fk.relname
--! поменять на pcl_pk.relnamespace::regnamespace::text||pcl_pk.relname
-- для 9.6

						replace(pc.conrelid::regclass::text,'.','')		allias_fk,
						replace(pc.confrelid::regclass::text,'.','')		allias_pk,
						pa_fk.attname	fk 

						from	pg_constraint	pc
						left join	pg_class 	pcl_fk		on	pc.conrelid = pcl_fk.oid
						left join	pg_class 	pcl_pk		on	pc.confrelid = pcl_pk.oid

						left join lateral
								(	
									select	array_agg(pa_pk.attname)	attname

										from	pg_attribute	pa_pk	
										where	pa_pk.attnum = any (pc.confkey)
											and	pc.conindid = pa_pk.attrelid

								)	pa_pk	on true
						left join lateral
								(	
									select	array_agg(pa_fk.attname)	attname

										from	pg_attribute	pa_fk	
										where	pa_fk.attnum = any (pc.conkey)
											and	pc.conrelid = pa_fk.attrelid
								)	pa_fk	on true
						where	pc.contype = 'f'	--only fk
				)	fk
	),
		sql_rec		as
		(	with recursive 	r	as
			(
				select	*

					from	sql_main	sm
				where 		table_pk = $$|| quote_nullable(_table) ||$$	--place for table full name

				union

				select	sm.*

					from	sql_main	sm
					join	r	on 	sm.table_pk = r.table_fk
			)
			select	*,
				row_number() over ()	r_n
				from	r
		),
		sql_result	as
		(
			select	sr.*,
				rank() over (partition by conname order by fk)	rank	--rank for join section 
				from	(
						select	
							r_n,
							table_fk,
/*count repeatitive tables for assign different allias*/allias_fk||row_number() over (partition by table_fk order by r_n)	allias_fk,	
							table_pk,
							allias_pk||1	allias_pk,	--join only to 1st table
							(unnest(pk))::text	pk,
							(unnest(fk))::text	fk,
							conname::text	conname

							from	sql_rec
					)	sr
			order by 	r_n
		)
$$;

	execute T_sql_sup||	$$

				select 	trim(trailing ',' from col_list)	col_list,
					join_list

					from	(
						select 	array_to_string(array_agg(join_list), E'\r ') 	join_list
							from	(	
								select 	case	rank
										when	1	
										then	' left join '|| table_fk ||' '|| allias_fk||' on '||allias_pk||'.'||pk||' = '||allias_fk||'.'||fk||' '	
										else	' and '||allias_pk||'.'||pk||' = '||allias_fk||'.'||fk||' '
									end	join_list
									from 	sql_result
								)	r
						)	join_list
					left join	(
								select	array_to_string(array_agg(col_list::text), E'\r ')	col_list
									from	(
											select	allias_fk||'.'||fk::text||' as '||allias_fk||fk||','	col_list
												from	sql_result
										)	r
							)	col_list	on true
				$$
	into	T_sql_col_list,
		T_sql_join_list;

	------------------------------------------------------------------------------------------
	-- Allias for main table. Concatenate '1' because of condition "join only to 1st table" --
	------------------------------------------------------------------------------------------
	execute $$(select replace($$||quote_nullable(_table)||$$,'.','')||'1')$$
	into	T_allias_mt;


-------------
--Main part--
-------------
T_sql_head := 	T_sql_sup	
		||$$
			select 	sql_head.r_n,
				sql_head.table_fk,
				sql_head.allias_fk,	
				sql_head.table_pk,
				sql_head.allias_pk,	
				sql_head.pk,
				sql_head.fk,
				sql_head.conname,
				sql_left_join.value_fk
				from 	sql_result 	sql_head 
		$$;	


T_sql_body := 	$$
				left join	(
							select	*
									from
								(
									select 	(each(hstore)).key	key_fk,
										(each(hstore)).value	value_fk

										from	(
												select	hstore(q)	
													from	(
															select 	$$|| T_sql_col_list || $$ 
																from 	$$|| _table ||$$ $$|| T_allias_mt ||$$ $$|| T_sql_join_list ||$$ 
																where 	$$||T_allias_mt||$$.$$|| _col ||$$=$$|| quote_nullable(_id) ||$$ 
														)	q
											)	q
								)	q
							where	value_fk is not null		
						)	sql_left_join	on	sql_head.allias_fk||fk = sql_left_join.key_fk
				where sql_left_join.value_fk is not null 
				group by 	1,2,3,4,5,6,7,8,9
				order by 	r_n	desc
		$$;

------------
--Exe part--
------------
	return QUERY execute T_sql_head||T_sql_body;

end;

/*
This support function for del_cascade.
Func return tree of foreing key with attr and values that prevent to delete a tuple in table (_table ['schema.table']) with identifier (_id).
Highly recommended to fill arg _col with pirmary key that present in table for query speed. If you dont do it it will be on your own risk.
*/

$body$
 LANGUAGE plpgsql stable;
ALTER FUNCTION del_cascade_support(text, text, bigint)
  OWNER TO postgres;

CREATE OR REPLACE FUNCTION del_cascade(_table text, _col text, _id bigint)
returns	void as
$body$
declare

	R_rec	record;

begin

	FOr R_rec in	select	*
				from	del_cascade_support(_table, _col, _id)

	loop

		EXecute	'DELETE FROM '|| R_rec.table_fk||' WHERE '||R_rec.fk||'::TEXT = '||R_rec.values_fk||'::TEXT;';

	end loop;

		EXecute	'DELETE FROM '|| _table ||' WHERE '||_col||' = '||_id||';';

end;
/*
Func delete all tuples that prevent to delete and make this from down to up (desc).
Use loop if your need to del several tuple.
*/
$body$
 LANGUAGE plpgsql volatile;
ALTER FUNCTION del_cascade(text, text, bigint)
  OWNER TO postgres;