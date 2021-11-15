-- Test expression pushdown in scans
SET yb_enable_expression_pushdown to on;

CREATE TABLE pushdown_test(k int primary key, i1 int, t1 text, d1 date, ts1 timestamp with timezone, r1 numrange, a1 int[]);

-- Simple expression (column = constant)

-- Simple function on one column

-- Simple function on multiple columns

-- Functions safe for pushdown

-- Typecast

-- Null test

-- Boolean expression

-- Aggregates

-- Parameter

-- Join

-- Negative test cases
-- Not immutable functions

-- Index scan

-- Custom datatype

-- Polymorphic types

-- Collation


DROP TABLE pushdown_test;