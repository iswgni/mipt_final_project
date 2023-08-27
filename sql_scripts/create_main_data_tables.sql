-- Размеры для varchar решил подобрать максимально подходящие под данныу, а не копировать из ddl_dml. 
-- Насколько я знаю от этого зависит скорость обработки данных.

-- Создание таблицы с заблокированными паспортами:
create table if not exists DWH_FACT_PASSPORT_BLACKLIST (
	passport_num varchar(32),
	entry_dt date
);


-- Создание таблицы клиентов:
create table if not exists DWH_DIM_CLIENTS (
	client_id varchar(32) primary key,
	last_name varchar(64),
	first_name varchar(64),
	patronymic varchar(64),
	date_of_birth date,
	passport_num varchar(32),
	passport_valid_to date,
	phone varchar(32),
	create_dt date,
	update_dt date
);


-- Создание таблицы аккаунтов:
create table if not exists DWH_DIM_ACCOUNTS (
	account_num varchar(32) primary key,
	valid_to date,
	client varchar(32),
	create_dt date,
	update_dt date,
	foreign key (client) references DWH_DIM_CLIENTS (client_id)
);


-- Создание таблицы карт:
create table if not exists DWH_DIM_CARDS (
    card_num varchar(32) primary key,
    account_num varchar(32),
    create_dt date,
    update_dt date,
    foreign key (account_num) references DWH_DIM_ACCOUNTS (account_num)
);


-- Создание таблицы терминалов:
create table if not exists DWH_DIM_TERMINALS_HIST (
	id integer primary key, 
	terminal_id varchar(32),
	terminal_type varchar(10),
	terminal_city varchar(64),
	terminal_address varchar(128),
	effective_from timestamp,
	effective_from_to timestamp,
	deleted_flg default 0
);


-- Создание таблицы транзакций:
create table if not exists DWH_FACT_TRANSACTIONS (
    trans_id varchar(32) primary key,
    trans_date date,
    amt decimal(10,2),
    card_num varchar(32),
    oper_type varchar(32),
    oper_result varchar(32),
    terminal varchar(32),
    foreign key (card_num) references DWH_DIM_CARDS (card_num),
    foreign key (terminal) references DWH_DIM_TERMINALS (terminal_id)
);


-- Создание таблицы отчётов:
create table if not exists REP_FRAUD (
	event_dt date,
	passport varchar(32),
	fio varchar(192),
	phone varchar(32),
	event_type varchar(128),
	report_dt date default current_timestamp
);


-- Создание стейджинг таблицы для транзакций:
create table if not exists STG_TRANSACTIONS (
	transaction_id varchar(32),
	transaction_date date,
	amount decimal(10,2),
	card_num varchar(32),
	oper_type varchar(32),
	oper_result varchar(32),
	terminal varchar(32)
);


-- Создание стейджинг таблицы для транзакций:
create table if not exists STG_PASSPORT_BLACKLIST (
	passport varchar(32),
	date date
);