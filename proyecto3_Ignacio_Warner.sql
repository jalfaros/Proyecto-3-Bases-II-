CREATE DATABASE proyecto3_basesii

CREATE EXTENSION dblink;
CREATE EXTENSION postgis;

CREATE SERVER leoviquez_remote
FOREIGN DATA WRAPPER dblink_fdw 
OPTIONS (host 'leoviquez.com', dbname 'p3_empresas', port '5432');

CREATE USER MAPPING FOR postgres
SERVER leoviquez_remote
OPTIONS (user 'basesII', password '12345');

GRANT USAGE ON FOREIGN SERVER leoviquez_remote to postgres;

SELECT dblink_connect('myconn', 'leoviquez_remote');
SELECT dblink_get_connections();
SELECT dblink_disconnect('myconn');

--CREANDO LA TABLA LOCAL
create table empresas(
	id_empresa		int primary key,
	nombre          varchar(300) not null
);
	
create table patentes (
	id_empresa int primary key ,
	tipo varchar (50) not null,
	permisos json null,
	constraint fk_patentes_empresa foreign key (id_empresa) references empresas(id_empresa)
);

CREATE TABLE empleados(
	id_empresa int primary key not null,
	empleados json null,
	CONSTRAINT fk_empleados_empresas foreign key (id_empresa) references empresas(id_empresa)
);

--Agregando columna geometry
SELECT AddGeometryColumn('public', 'empresas', 'geom', 4326, 'POINT', 2);

--Creando vista de proyectos locales



create view vista_empresa_local
as 
	select em.*, p.tipo, p.permisos, e.empleados
	from empresas em inner join empleados e on em.id_empresa = e.id_empresa
	inner join patentes p on e.id_empresa = p.id_empresa;

--Creando funcion para actualizar en nodo central y local
CREATE OR REPLACE FUNCTION update_vista_empresas()
    RETURNS trigger
    LANGUAGE 'plpgsql'
AS 
$BODY$
declare
	sql               varchar(2000);
	valor_innecesario int;
	detalles   		  varchar(500);
BEGIN	
	perform dblink_connect('myconn', 'leoviquez_remote');
	perform dblink('myconn','begin');
	
	detalles := json_build_object('tipo',new.tipo,'permisos',new.permisos,'empleados',new.empleados);
	sql := 'select registra_caracteristicas (' || new.id_empresa || ',''' || detalles || ''',''warner'',''buho'')';
	select * into valor_innecesario from dblink('myconn',sql) as result(respuesta int);
	raise notice 'Detalles registrados en servidor central';
	if(exists(select id_empresa from empresas where empresas.id_empresa = new.id_empresa))
	then
		update empresas  set nombre    =NEW.nombre, geom   =NEW.geom     where id_empresa=NEW.id_empresa;
		update patentes  set tipo      =NEW.tipo, permisos =NEW.permisos where id_empresa=NEW.id_empresa;
		update empleados set empleados =NEW.empleados                    where id_empresa=NEW.id_empresa;
		raise notice 'Empresa actualizada en nodo local';
	end if;
	
	perform dblink_exec('myconn','end;');
	perform dblink_disconnect('myconn');
RETURN NEW;
END;
$BODY$;


--Trigger para la función de actualizar empresa
CREATE TRIGGER trigger_update_vista_empresas
INSTEAD OF update
ON  vista_empresa_local
FOR EACH ROW
EXECUTE PROCEDURE update_vista_empresas();


--Recuperación de las empresas loscales y las del servidor central
select c.id,c.nombre,el.tipo,el.permisos from vista_empresa_local el right outer join (
	select * 
	from dblink('myconn','select id,nombre from s_data.empresas') 
	as respuesta(id int,nombre varchar)) c on (el.id_empresa=c.id)


--Creando funcion para insertar en nodo central y local
create or replace function insertar_vista_empresas()
	returns trigger
	language 'plpgsql'
as
$BODY$
declare
	sql 	   		  varchar(2000);
	detalles   		  varchar(500);
	id_empresa 		  int;
	valor_innecesario int;
begin
	perform dblink_connect('myconnn', 'leoviquez_remote');
	perform dblink('myconnn','begin');
	
	sql := 'SELECT crea_empresa (''' || new.nombre || ''',''' || st_astext(new.geom) || ''', ''warner'',''buho'')';
	
	select * into id_empresa from dblink('myconnn',sql) as respuesta(id int);
	raise notice 'Empresa registrada en servidor central (id:%)',id_empresa;
	
	detalles := json_build_object('tipo',new.tipo,'permisos',new.permisos,'usuario',new.empleados);
	sql := 'select registra_caracteristicas (' || id_empresa || ',''' || detalles || ''',''warner'',''buho'')';
	select * into valor_innecesario from dblink('myconnn',sql) as result(respuesta int);
	raise notice 'Detalles registrados en servidor central';
	
	insert into empresas(id_empresa,nombre,geom) values  (id_empresa, new.nombre, new.geom);
	insert into patentes(id_empresa,tipo,permisos) values (id_empresa, new.tipo, new.permisos);
	insert into empleados(id_empresa,empleados) values (id_empresa, new.empleados);
	
	perform dblink_exec('myconnn','end;');
	perform dblink_disconnect('myconnn');
	
return new;
end;
$BODY$;

select dblink_disconnect('myconn');


--CREANDO EL TRIGGER PARA EJECUTAR LA FUNCIÓN
drop trigger trigger_insert_vista_empresas on vista_empresa_local

create trigger trigger_insert_vista_empresas
instead of insert
on vista_empresa_local
for each row
execute procedure insertar_vista_empresas();



-- Trigger de prueba que elimina los datos de la vista


CREATE OR REPLACE FUNCTION eliminar_empresa()
	RETURNS TRIGGER 
	LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
	sql varchar(200);
	valor_innecesario int;
BEGIN
		
		DELETE FROM empleados where id_empresa = old.id_empresa;
		DELETE FROM patentes where id_empresa = old.id_empresa;
		DELETE FROM empresas where id_empresa = old.id_empresa;

		perform dblink_connect('trigger_eliminar', 'leoviquez_remote');
		perform dblink('trigger_eliminar','begin');

		sql := 'SELECT elimina_empresa(''' || old.id_empresa || ''', ''warner'', ''buho'' )';
		--select dblink('trigger_eliminar', sql);
		select * into valor_innecesario from dblink('trigger_eliminar',sql) as result(respuesta int);
		raise notice 'Eliminado pegguo';

		perform dblink_exec('trigger_eliminar', 'end');
		perform dblink_disconnect('trigger_eliminar');
return old; 
END;
$BODY$;

create trigger trigger_eliminar_vista_empresas
instead of delete
on vista_empresa_local
for each row
execute procedure eliminar_empresa();


--Creando la vista que reuna todo (vista central + vista local)

create view vista_empresa_central_local
	as
	select c.id , c.nombre, c.atributos, c.locali from vista_empresa_local el right outer join (
	select * 
	from dblink('dbname=p3_empresas port=5432 host=leoviquez.com user=basesII password=12345','select * from vista_empresas') 
	as respuesta(id int,nombre varchar, locali geometry, atributos json)) c on (el.id_empresa = c.id) order by c.id desc;



-- Pruebas para la entrega (video)

select * from empresas;
select * from empleados;
select * from patentes;
select * from vista_empresa_local
select * from vista_empresa_central_local order by id desc LIMIT 10