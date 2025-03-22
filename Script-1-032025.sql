/*ETL
1.создала в postgres новую базу данных DWH
2.в базе данных DWH создала 3 схемы Stage, Detail, Mart
3.в схеме Stage создала таблицы Source_1, Source_2, Source_3
  структура таблицы 
  все поля 
  - как в Excel файлах трех шпаргалок тип TEXT (у таблиц некоторые поля могут быть аналогичными, некоторые могут отсутствовать, т к они велись разными людьми)
4.сохранила Excel файлы шпаргалок  в формате *.csv (CSV кодирование UTF-8 на тот же сервер, что и БД) 
** примечание: только англоязычные названия таблиц и столбцов
структурированный EXEL лист (если больше 1 листа-не сохранит) 

5.используя команду COPY, загружаем файл *.csv в таблицу Source_1
в результате должны увидеть все записи Excel файла в таблице Source_1*/

-- создание table stage.Source_1

create table stage.Source_1
(theme text,
function_name text,
what_is_doing text,
SUBD text,
info text,
example_1 text,
example_2 text,
example_3 text,
example_4 text,
example_5 text, 
loaded timestamp default current_timestamp);

-- копирование из CSV d table stage.Source_1 :

copy stage.Source_1 
(theme,
function_name,
what_is_doing,
SUBD,
info,
example_1,
example_2,
example_3,
example_4,
example_5)
FROM 'C:\Program Files\PostgreSQL\17\knowlegeSQL_csv.csv' 
with(format csv, header true, DELIMITER ';', encoding UTF8);

-- создание table stage.Sourse_2

create table stage.Sourse_2
(theme text,
function_name text,
what_is_doing text,
parameters text,
info text,
examples text,
difficulty_understanding text,
difficulty_using text,
basic text,
replaced_by text,
loaded timestamp default current_timestamp);

-- копирование из CSV d table stage.Source_2 :

copy stage.Sourse_2
(theme,
function_name,
what_is_doing,
parameters,
info,
examples,
difficulty_understanding,
difficulty_using,
basic,
replaced_by )
from 'C:\Program Files\PostgreSQL\17\Sql function list.csv'
with (format csv, header true, delimiter ';');

-- создание table stage.Sourse_3:
create table stage.Sourse_3
(function_type text,
description text,
function_mysql text,
argument_func_mysql text,
function_postgresql text,
argument_func_postgresql text,
returns text,
detailed_description text,
examples_mysql text,
examples_postgresql text,
replacing text,
loaded timestamp default current_timestamp);

-- копирование из CSV d table stage.Source_3 :

copy stage.Sourse_3(
function_type,
description,
function_mysql,
argument_func_mysql,
function_postgresql,
argument_func_postgresql,
returns,
detailed_description,
examples_mysql,
examples_postgresql,
replacing)
from 'C:\FOR DB\Note1 utf sql.csv'
with (format csv, header true, delimiter ';');

/*создание гибридной таблицы (если поля в разных таблицах называются по-разному, но отражают однотипную информацию-
в гибридной таблице эти данные в общий столбик собираю, если в какой-то таблице есть поля, которых нет в остальных и данный тип информации другими не вносился-
это поле должно быть в гибридной таблице. Т.е., структура гибридной таблицы должна вмещать ВСЮ информацию из трех таблиц)*/

-- создание:
create table stage.source_hybrid_as_is
(function_type text,
	function_name text,
	what_is_doing text,
	subd text,
	parameters text,
	info text,
	examples text,	
	difficulty_understanding text,
	difficulty_using text,
	basic text,
	replaced_by text,
	loaded timestamp);

-- перенос данных из Source_1 в source_hybrid_as_is:

INSERT INTO stage.source_hybrid_as_is (function_type, function_name, what_is_doing, subd, info ,examples, loaded) SELECT theme, 
function_name, 
what_is_doing, 
subd, 
info, 
CONCAT_ws ('*****', example_1 ,	example_2 ,	example_3 ,	example_4 ,	example_5), -- все примеры собрали в общую ячейку 
loaded
from stage.Source_1;

-- перенос данных из Source_2 в source_hybrid_as_is:

INSERT INTO stage.source_hybrid_as_is (function_type,
	function_name,
	what_is_doing,
	subd,
	parameters,
	info ,
	examples,	
	difficulty_understanding,
	difficulty_using,
	basic,
	replaced_by,
	loaded)
	SELECT theme,
	function_name,
	what_is_doing, 'my_sql',
	parameters,	
	info,
	examples,
	difficulty_understanding,
	difficulty_using,
	basic,
	replaced_by, 
	loaded 
from stage.sourse_2;

-- перенос данных из Source_3 в source_hybrid_as_is:

INSERT INTO stage.source_hybrid_as_is 
(function_type ,
	function_name,
	what_is_doing,
	subd,
	parameters,
	info,
	examples,
	replaced_by)	
	SELECT function_type,
	function_mysql,
	description,
	'my_sql',
	argument_func_mysql, 
	CONCAT('возвращает ', returns,' ,', detailed_description),
	examples_mysql,
	replacing, 
	loaded
	from stage.sourse_3
	WHERE function_mysql IS NOT NULL 
	union ALL
	SELECT function_type,
	function_postgresql,
	description,
	'postgresql',
	argument_func_postgresql, 
	CONCAT('возвращает ', returns,', ', detailed_description), --поле returns присоединила к info
	examples_postgresql,
	replacing,
	loaded
	from stage.sourse_3
	WHERE function_postgresql IS NOT NULL; 
	
	
	/*для подготовки перевода колонки basic в boolean переводим значения(да/нет) на английский*/
	
	update stage.source_hybrid_as_is
set basic = case UPPER(basic) 
				when 'ДА' then 'Yes'
				when 'НЕТ' then 'No'
			end
where basic is not null;


/*Создание справочников в схеме detail. Посмотрела гибридную таблицу, визуально оценила поля, 
где разнообразие значений не большое и они повторяются. На эти поля сделала запросы: */

/*??????ВОПРОС 1???? так как одинаковые по смыслу понятия могут писаться по-разному,в том числе с ошибками. 
Я бы сейчас перед созданием справочника (после выгрузки уникальных значений) сделала бы UPDATE гибридной таблицы и привела 
бы все односмысловые значения к однообразному виду. После этого значения в справочник можно внести автоматически и заполнять поля с внешними ключами проще. 
Сейчас я заполняла справочники вручную, выбирая из односмысловых уникальных один вариант. Заполнять справочники вручную неправильно?*/
/*?????ВОПРОС 2????? что лучше использовать а поле id справочника :INT или SERIAL?? (с serial могут быть проблемы при всяких изменениях но он удобнее заполняется)

select distinct function_type
from source_hybrid;

select distinct subd
from source_hybrid;

select distinct difficulty_understanding

from source_hybrid;


select distinct difficulty_using 
from source_hybrid;

-- на 4 столбца сделала 3 справочника: 


CREATE TABLE detail.dbms (
	id int4 NOT NULL,
	dbms_type varchar(20) ,
	CONSTRAINT dbms_pkey PRIMARY KEY (id)
);

insert into detail.DBMS
values (1, 'PostgreSQL'),
(2, 'MySQL'),
(3, 'Oracle');


CREATE TABLE detail.difficulty (
	id int4 NOT NULL,
	d_level varchar(20),
	CONSTRAINT diff_understanding_pkey PRIMARY KEY (id)
);
insert into detail.difficulty
values (1, 'низкая'),
(2, 'средняя'),
(3, 'выше средней');


CREATE TABLE detail.function_type_list (
	id int4 NOT NULL,
	function_type varchar(20) ,
	CONSTRAINT function_type_list_pkey PRIMARY KEY (id)
);

insert into detail.function_type_list
values (1,'агрегатные'),
(2,'ранжирующие оконные'), 
(3,'текстовые'), 
(4,'оконные'), 
(5,'дополнительные'), 
(6,'дата и время'), 
(7,'создание таблиц'), 
(8,'удаление записей'), 
(9,'внесение записей'), 
(10,'обновление таблиц'),
(11,'числовые');


/*Создаю общую таблицу в схеме detail, которая содержит все поля из гибридной таблицы 
кроме 
function_type varchar(40),
subd text, 
difficulty_understanding text, 
difficulty_using text, 
replaced_by text (вместо них ссылки на справочнки),
плюс 
effect_from timestamp default not NULL, 
effect_to timestamp default '2222-12-31 23:59:59', 
processed default CURRENT_TIMESTAMP 
id - serial, 
+поля-внешние ключи, которые ссылаются на справочники, 
+ поле replaced_id, 
которое ссылается на id функции из данной таблицы, которая может заменить ту, о которой говориться в данной записи
*/

create table detail.common_table
(	id SERIAL,
	function_type_id INT references detail.function_type_list(id),
	function_name varchar(20),
	what_is_doing varchar(1000),
	dbms_id INT references detail.dbms(id),
	parameters varchar(1000),
	info text,
	examples text,	
	difficulty_understanding_level INT references detail.difficulty(id),
	difficulty_using_level INT references detail.difficulty(id),
	basic boolean,
	replaced_by varchar(20),
	effect_from timestamp, 
	effect_to timestamp default '2222-12-31 23:59:59',
	deleted_flag boolean default false,
	processed timestamp default CURRENT_TIMESTAMP,
	PRIMARY KEY(id, effect_from));


-- перенос данных из гибридной таблицы (обязательно перечисляю поля, в которые "перекачевывают" и которые "перекачевывают" ):

insert into detail.common_table (
	function_name,
	what_is_doing ,
	parameters ,
	info ,
	examples ,	
	basic, 
	replaced_by,
	effect_from)
select function_name::varchar(20),	what_is_doing::varchar(1000),
	parameters::varchar(1000),
	info,
	examples,	
	basic::boolean,
	replaced_by::varchar(20),
	loaded 
	from stage.source_hybrid_as_is;

-- вношу в общую таблицу в поле function_type_id данные внешнего ключа к справочнику detail.function_type_list:

update detail.common_table as c 
set function_type_id=f.id
from detail.function_type_list as f RIGHT JOIN stage.source_hybrid_as_is as s ON UPPER(left(s.function_type,5))=UPPER(left(f.function_type,5)) 
or UPPER(left(s.function_type,2))=UPPER(left(f.function_type,1)||'/')
where c.function_name=s.function_name and c.loaded=s.effect_from; 


select type_id from detal.type where detail.type.name = stage.sql_functions.function_type


/*из-за вариабельности написания такое равенство, если бы изначально привести записи к однообразию, можно было бы ставить = прости между полями*/


-- вношу в общую таблицу в поле dbms_id данные внешнего ключа к справочнику detail.dbms:

update detail.common_table as c
set dbms_id=d.id
from detail.dbms as d 
where UPPER(left(c.subd,2))=UPPER(left(d.dbms_type,2));


-- вношу в общую таблицу в поле difficulty_understanding_level данные внешнего ключа к справочнику detail.difficulty:

update detail.common_table as c
set difficulty_understanding_level=di.id
from detail.difficulty as di
where UPPER(left(c.difficulty_understanding ,4))=UPPER(left(di.d_level ,4));


-- вношу в общую таблицу в поле difficulty_using_level данные внешнего ключа к справочнику detail.difficulty:

update detail.common_table as c
set difficulty_using_level=di.id
from detail.difficulty as di
where UPPER(left(c.difficulty_using ,4))=UPPER(left(di.d_level ,4))


-- вношу в общую таблицу в поле replaced_id данные внешнего ключа к этой же таблице к полю id

update detail.common_table as c
set replaced_id=c2.id 
from detail.common_table as c1 inner join  detail.common_table c2 
on UPPER(left(c1.replaced_by,5))=UPPER(left(c2.function_name,5))
where c.id=c1.id;


/*удаляю столбцы, в которых текстовая информация дублирует то, что отражают значения, полученные при джоинах или подзапросах по внешним ключам:*/

alter table detail.common_table
drop column function_type ,	drop column subd ,
	drop column difficulty_understanding,
	drop column difficulty_using, drop column replaced_by;


*/ При необходимости можно обратно собрать общую таблицу:*/

select ct.id, f.function_type,	
	ct.function_name,	
	ct.what_is_doing, 
	d.dbms_type,	
	ct.parameters,
	ct.info,
	ct.examples,	
	di1.d_level as difficulty_understanding,
	di2.d_level as difficulty_using,
	ct.basic text,
	c2.function_name as replaced_by
from detail.dbms d right join detail.common_table ct on d.id=ct.dbms_id 
left join detail.difficulty di1 on ct.difficulty_understanding_level=di1.id
left join detail.difficulty di2 on ct.difficulty_using_level=di2.id
left join detail.common_table c2 on ct.replaced_id=c2.id
left join detail.function_type_list f on ct.function_type_id=f.id 
ORDER BY ct.id;





