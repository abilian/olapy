--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.5
-- Dumped by pg_dump version 9.6.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: geography; Type: TABLE; Schema: public; Owner: tutorial
--

CREATE TABLE geography (
    geography_key integer,
    continent text,
    country text,
    city text
);


ALTER TABLE geography OWNER TO tutorial;

--
-- Name: product; Type: TABLE; Schema: public; Owner: tutorial
--

CREATE TABLE product (
    product_key integer,
    company text,
    article text,
    licence text
);


ALTER TABLE product OWNER TO tutorial;

--
-- Name: sales_facts; Type: TABLE; Schema: public; Owner: tutorial
--

CREATE TABLE sales_facts (
    geography_key integer,
    product_key integer,
    amount integer,
    count integer
);


ALTER TABLE sales_facts OWNER TO tutorial;

--
-- Data for Name: geography; Type: TABLE DATA; Schema: public; Owner: tutorial
--

COPY geography (geography_key, continent, country, city) FROM stdin;
1	America	Canada	Quebec
2	America	Canada	Toronto
3	America	United States	Los Angeles
4	America	United States	New York
5	America	United States	San Francisco
6	America	Mexico	Mexico
7	America	Venezuela	Caracas
8	Europe	France	Paris
9	Europe	Spain	Barcelona
10	Europe	Spain	Madrid
\.


--
-- Data for Name: product; Type: TABLE DATA; Schema: public; Owner: tutorial
--

COPY product (product_key, company, article, licence) FROM stdin;
1	Crazy Development	olapy	Corporate
2	Crazy Development	olapy	Partnership
3	Crazy Development	olapy	Personal
4	Crazy Development	olapy	Startup
\.


--
-- Data for Name: sales_facts; Type: TABLE DATA; Schema: public; Owner: tutorial
--

COPY sales_facts (geography_key, product_key, amount, count) FROM stdin;
1	3	1	84
2	3	2	841
3	3	4	2
4	3	8	231
5	1	16	4
6	2	32	65
7	2	64	64
8	1	128	13
9	1	256	12
10	1	512	564
\.


--
-- PostgreSQL database dump complete
--

