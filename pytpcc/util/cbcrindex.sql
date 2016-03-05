drop index CUSTOMER.CU_ID_D_ID_W_ID USING GSI;
drop index CUSTOMER.CU_W_ID_D_ID_LAST USING GSI;
drop index DISTRICT.DI_ID_W_ID using gsi;

drop index ITEM.IT_ID using gsi

drop index NEW_ORDER.NO_D_ID_W_ID using gsi;
drop index ORDERS.OR_O_ID_D_ID_W_ID using gsi;
drop index ORDERS.OR_W_ID_D_ID_C_ID using gsi;
drop index ORDER_LINE.OL_O_ID_D_ID_W_ID using gsi;
drop index STOCK.ST_W_ID_I_ID1 using gsi;

drop index WAREHOUSE.WH_ID using gsi;

drop primary index on CUSTOMER using gsi;
drop primary index on DISTRICT using gsi;
drop primary index on HISTORY gsi;
drop primary index on ITEM using gsi;
drop primary index on NEW_ORDER using gsi;
drop primary index on ORDERS using gsi;
drop primary index on ORDER_LINE using gsi;
drop primary index on STOCK using gsi;
drop primary index on WAREHOUSE using gsi;

create index CU_ID_D_ID_W_ID on CUSTOMER(C_ID, C_D_ID, C_W_ID) using gsi ;;
create index CU_W_ID_D_ID_LAST on CUSTOMER(C_W_ID, C_D_ID, C_LAST) using gsi ;;

create index DI_ID_W_ID on DISTRICT(D_ID, D_W_ID) using gsi ;

create index IT_ID on ITEM(I_ID) using gsi ;

create index NO_D_ID_W_ID on NEW_ORDER(NO_O_ID, NO_D_ID, NO_W_ID) using gsi ;


create index OR_O_ID_D_ID_W_ID on ORDERS(O_ID, O_D_ID, O_W_ID, O_C_ID) using gsi ;
create index OR_W_ID_D_ID_C_ID on ORDERS(O_W_ID, O_D_ID, O_C_ID) using gsi ;


create index OL_O_ID_D_ID_W_ID on ORDER_LINE(OL_O_ID, OL_D_ID, OL_W_ID) using gsi ;

create index ST_W_ID_I_ID1 on STOCK(S_W_ID, S_I_ID) using gsi ;

create index WH_ID on WAREHOUSE(W_ID) using gsi ;

/**** There indices are unnecessary for PY-TPCC.  Only needed for debugging.
create primary index PX_CUSTOMER on CUSTOMER using gsi ;;
create primary index PX_DISTRICT on DISTRICT using gsi ;
create primary index PX_HISTORY on HISTORY using gsi;
create primary index PX_ITEM on ITEM using gsi ;
create primary index PX_NEW_ORDER on NEW_ORDER using gsi ;
create primary index PX_ORDERS on ORDERS using gsi ;
create primary index PX_ORDER_LINE on ORDER_LINE using gsi ;
create primary index PX_STOCK on STOCK using gsi ;
create primary index PX_WAREHOUSE on WAREHOUSE using gsi ;


These queries will give you the list of indices and their status.
select keyspace_id, state from system:indexes;
select keyspace_id, state from system:indexes where state != 'online';

****/
