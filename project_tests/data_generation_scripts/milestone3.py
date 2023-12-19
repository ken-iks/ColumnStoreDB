#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd
import math

import data_gen_utils

# note this is the base path where we store the data files we generate
TEST_BASE_DIR = "/cs165/generated_data"

# note this is the base path that _POINTS_ to the data files we generate
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

#
# Example usage: 
#   python milestone3.py 10000 42 ~/repo/cs165-docker-test-runner/test_data /cs165/staff_test
#

############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
# 
############################################################################

# PRECISION FOR AVG OPERATION. See 
PLACES_TO_ROUND = 2
np.set_printoptions(formatter={'float': lambda x: "{0:0.2f}".format(x)})


def generateDataMilestone3(dataSize):
    outputFile_ctrl = TEST_BASE_DIR + '/' + 'data4_ctrl.csv'
    outputFile_btree = TEST_BASE_DIR + '/' + 'data4_btree.csv'
    outputFile_clustered_btree = TEST_BASE_DIR + '/' + 'data4_clustered_btree.csv'
    header_line_ctrl = data_gen_utils.generateHeaderLine('db1', 'tbl4_ctrl', 4)
    header_line_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4', 4)
    header_line_clustered_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4_clustered_btree', 4)
    outputTable = pd.DataFrame(np.random.randint(0, dataSize/5, size=(dataSize, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    # This is going to have many, many duplicates for large tables!!!!
    outputTable['col1'] = np.random.randint(0,1000, size = (dataSize))
    outputTable['col4'] = np.random.randint(0,10000, size = (dataSize))
    ### make ~5\% of values a single value! 
    maskStart = np.random.uniform(0.0,1.0, dataSize)   
    mask1 = maskStart < 0.05
    ### make ~2% of values a different value
    maskStart = np.random.uniform(0.0,1.0, dataSize)
    mask2 = maskStart < 0.02
    outputTable['col2'] = np.random.randint(0,10000, size = (dataSize))
    frequentVal1 = np.random.randint(0,int(dataSize/5))
    frequentVal2 = np.random.randint(0,int(dataSize/5))
    outputTable.loc[mask1, 'col2'] = frequentVal1
    outputTable.loc[mask2, 'col2'] = frequentVal2
    outputTable['col4'] = outputTable['col4'] + outputTable['col1']
    outputTable.to_csv(outputFile_ctrl, sep=',', index=False, header=header_line_ctrl, lineterminator='\n')
    outputTable.to_csv(outputFile_btree, sep=',', index=False, header=header_line_btree, lineterminator='\n')
    outputTable.to_csv(outputFile_clustered_btree, sep=',', index=False, header=header_line_clustered_btree, lineterminator='\n')
    return frequentVal1, frequentVal2, outputTable

def createTest20():
    # prelude
    output_file, exp_output_file = data_gen_utils.openFileHandles(20, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Create a control table that is identical to the one in test21.dsl, but\n')
    output_file.write('-- without any indexes\n')
    output_file.write('--\n')
    output_file.write('-- Loads data from: data4_ctrl.csv\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl4_ctrl",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl4_ctrl)\n')
    output_file.write('create(col,"col2",db1.tbl4_ctrl)\n')
    output_file.write('create(col,"col3",db1.tbl4_ctrl)\n')
    output_file.write('create(col,"col4",db1.tbl4_ctrl)\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data4_ctrl.csv\")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data and their indexes are durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTest21():
    output_file, exp_output_file = data_gen_utils.openFileHandles(21, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Test for creating table with indexes\n')
    output_file.write('--\n')
    output_file.write('-- Table tbl4 has a clustered index with col3 being the leading column.\n')
    output_file.write('-- The clustered index has the form of a sorted column.\n')
    output_file.write('-- The table also has an unclustered btree index on col2.\n')
    output_file.write('--\n')
    output_file.write('-- Loads data from: data4_btree.csv\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl4",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl4)\n')
    output_file.write('create(col,"col2",db1.tbl4)\n')
    output_file.write('create(col,"col3",db1.tbl4)\n')
    output_file.write('create(col,"col4",db1.tbl4)\n')
    output_file.write('-- Create a clustered index on col3\n')
    output_file.write('create(idx,db1.tbl4.col3,sorted,clustered)\n')
    output_file.write('-- Create an unclustered btree index on col2\n')
    output_file.write('create(idx,db1.tbl4.col2,btree,unclustered)\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately in the form of a clustered index\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data4_btree.csv\")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data and their indexes are durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTests22And23(dataTable, dataSize):
    output_file22, exp_output_file22 = data_gen_utils.openFileHandles(22, TEST_DIR=TEST_BASE_DIR)
    output_file23, exp_output_file23 = data_gen_utils.openFileHandles(23, TEST_DIR=TEST_BASE_DIR)
    output_file22.write('--\n')
    output_file22.write('-- Query in SQL:\n')
    # selectivity = 
    offset = np.max([1, int(dataSize/5000)])
    offset2 = np.max([2, int(dataSize/2500)])
    val1 = np.random.randint(0, int((dataSize/5) - offset))
    val2 = np.random.randint(0, int((dataSize/5) - offset2))
    # generate test 22
    output_file22.write('-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= {} and col3 < {};\n'.format(val1, val1+offset))
    output_file22.write('-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= {} and col3 < {};\n'.format(val2, val2+offset2))
    output_file22.write('--\n')
    output_file22.write('s1=select(db1.tbl4_ctrl.col3,{},{})\n'.format(val1, val1 + offset))
    output_file22.write('f1=fetch(db1.tbl4_ctrl.col1,s1)\n')
    output_file22.write('print(f1)\n')
    output_file22.write('s2=select(db1.tbl4_ctrl.col3,{},{})\n'.format(val2, val2 + offset2))
    output_file22.write('f2=fetch(db1.tbl4_ctrl.col1,s2)\n')
    output_file22.write('print(f2)\n')
    # generate test 23
    output_file23.write('--\n')
    output_file23.write('-- tbl3 has a secondary b-tree tree index on col2, and a clustered index on col3 with the form of a sorted column\n')
    output_file23.write('-- testing for correctness\n')
    output_file23.write('--\n')
    output_file23.write('-- Query in SQL:\n')
    output_file23.write('-- SELECT col1 FROM tbl4 WHERE col3 >= {} and col3 < {};\n'.format(val1, val1+offset))
    output_file23.write('-- SELECT col1 FROM tbl4 WHERE col3 >= {} and col3 < {};\n'.format(val2, val2+offset2))
    output_file23.write('--\n')
    output_file23.write('-- since col3 has a clustered index, the index is expected to be used by the select operator\n')
    output_file23.write('s1=select(db1.tbl4.col3,{},{})\n'.format(val1, val1 + offset))
    output_file23.write('f1=fetch(db1.tbl4.col1,s1)\n')
    output_file23.write('print(f1)\n')
    output_file23.write('s2=select(db1.tbl4.col3,{},{})\n'.format(val2, val2 + offset2))
    output_file23.write('f2=fetch(db1.tbl4.col1,s2)\n')
    output_file23.write('print(f2)\n')
    # generate expected results
    dfSelectMask1 = (dataTable['col3'] >= val1) & (dataTable['col3'] < (val1 + offset))
    dfSelectMask2 = (dataTable['col3'] >= val2) & (dataTable['col3'] < (val2 + offset2))
    output1 = dataTable[dfSelectMask1]['col1']
    output2 = dataTable[dfSelectMask2]['col1']
    for exp_output_file in [exp_output_file22, exp_output_file23]:
        exp_output_file.write(data_gen_utils.outputPrint(output1))
        exp_output_file.write('\n\n')
        exp_output_file.write(data_gen_utils.outputPrint(output2))
        exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file22, exp_output_file22)
    data_gen_utils.closeFileHandles(output_file23, exp_output_file23)

def createTest24(dataTable, dataSize):
    output_file, exp_output_file = data_gen_utils.openFileHandles(24, TEST_DIR=TEST_BASE_DIR)
    offset = np.max([1, int(dataSize/10)])
    offset2 = 2000
    val1 = np.random.randint(0, int((dataSize/5) - offset))
    val2 = np.random.randint(0, 8000)
    output_file.write('-- Test for a clustered index select followed by a second predicate\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(col1) FROM tbl4 WHERE (col3 >= {} and col3 < {}) AND (col2 >= {} and col2 < {});\n'.format(val1, val1+offset, val2, val2+offset2))
    output_file.write('--\n')
    output_file.write('s1=select(db1.tbl4.col3,{},{})\n'.format(val1, val1 + offset))
    output_file.write('f1=fetch(db1.tbl4.col2,s1)\n')
    output_file.write('s2=select(s1,f1,{},{})\n'.format(val2, val2 + offset2))
    output_file.write('f2=fetch(db1.tbl4.col1,s2)\n')
    output_file.write('print(f2)\n')
    output_file.write('a1=sum(f2)\n')
    output_file.write('print(a1)\n')
    # generate expected results
    dfSelectMask1Low = dataTable['col3'] >= val1
    dfSelectMask1High = dataTable['col3'] < (val1 + offset)
    dfSelectMask2Low = dataTable['col2'] >= val2
    dfSelectMask2High = dataTable['col2'] < (val2 + offset2)
    dfTotalMask = dfSelectMask1Low & dfSelectMask1High & dfSelectMask2Low & dfSelectMask2High
    values = dataTable[dfTotalMask]['col1']
    exp_output_file.write(data_gen_utils.outputPrint(values))
    exp_output_file.write('\n\n')
    exp_output_file.write(str(values.sum()) + '\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTests25And26(dataTable, dataSize):
    output_file25, exp_output_file25 = data_gen_utils.openFileHandles(25, TEST_DIR=TEST_BASE_DIR)
    output_file26, exp_output_file26 = data_gen_utils.openFileHandles(26, TEST_DIR=TEST_BASE_DIR)
    offset = np.max([2, int(dataSize/1000)])
    output_file25.write('-- Test for a non-clustered index select followed by an aggregate (control-test)\n')
    output_file25.write('--\n')
    output_file25.write('-- Query form in SQL:\n')
    output_file25.write('-- SELECT avg(col3) FROM tbl4_ctrl WHERE (col2 >= _ and col2 < _);\n')
    output_file25.write('--\n')
    output_file26.write('-- Test for a non-clustered index select followed by an aggregate (control-test)\n')
    output_file26.write('--\n')
    output_file26.write('-- Query form in SQL:\n')
    output_file26.write('-- SELECT sum(col3) FROM tbl4 WHERE (col2 >= _ and col2 < _);\n')
    output_file26.write('--\n')
    for i in range(10):
        val1 = np.random.randint(0, int((dataSize/5) - offset))
        output_file25.write('s{}=select(db1.tbl4_ctrl.col2,{},{})\n'.format(i, val1, val1 + offset))
        output_file25.write('f{}=fetch(db1.tbl4_ctrl.col3,s{})\n'.format(i,i))
        output_file25.write('a{}=sum(f{})\n'.format(i,i))
        output_file25.write('print(a{})\n'.format(i))
        output_file26.write('s{}=select(db1.tbl4.col2,{},{})\n'.format(i, val1, val1 + offset))
        output_file26.write('f{}=fetch(db1.tbl4.col3,s{})\n'.format(i,i))
        output_file26.write('a{}=sum(f{})\n'.format(i,i))
        output_file26.write('print(a{})\n'.format(i))
        # generate expected results
        dfSelectMask1 = (dataTable['col2'] >= val1) & (dataTable['col2'] < (val1 + offset))
        values = dataTable[dfSelectMask1]['col3']
        sum_result = values.sum()
        if (math.isnan(sum_result)):
            exp_output_file25.write('0\n')
            exp_output_file26.write('0\n')
        else:
            exp_output_file25.write(str(sum_result) + '\n')
            exp_output_file26.write(str(sum_result) + '\n')
    data_gen_utils.closeFileHandles(output_file25, exp_output_file25)
    data_gen_utils.closeFileHandles(output_file26, exp_output_file26)

def createTest27(dataTable, frequentVal1, frequentVal2):
    output_file, exp_output_file = data_gen_utils.openFileHandles(27, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Test for a clustered index select followed by a second predicate\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= {} and col2 < {});\n'.format(frequentVal1 - 1, frequentVal1 + 1))
    output_file.write('-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= {} and col2 < {});\n'.format(frequentVal2 - 1, frequentVal2 + 1))
    output_file.write('--\n')
    output_file.write('s1=select(db1.tbl4.col2,{},{})\n'.format(frequentVal1 - 1, frequentVal1 + 1))
    output_file.write('f1=fetch(db1.tbl4.col1,s1)\n')
    output_file.write('a1=sum(f1)\n')
    output_file.write('print(a1)\n')
    output_file.write('s2=select(db1.tbl4.col2,{},{})\n'.format(frequentVal2 - 1, frequentVal2 + 1))
    output_file.write('f2=fetch(db1.tbl4.col1,s2)\n')
    output_file.write('a2=sum(f2)\n')
    output_file.write('print(a2)\n')
    # generate expected results
    dfSelectMask1 = (dataTable['col2'] >= (frequentVal1 - 1)) & (dataTable['col2'] < (frequentVal1 + 1))
    result1 = dataTable[dfSelectMask1]['col1'].sum()
    dfSelectMask2 = (dataTable['col2'] >= (frequentVal2 - 1)) & (dataTable['col2'] < (frequentVal2 + 1))
    result2 = dataTable[dfSelectMask2]['col1'].sum()
    exp_output_file.write(str(result1) + '\n')
    exp_output_file.write(str(result2) + '\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTests28And29(dataTable, dataSize):
    output_file28, exp_output_file28 = data_gen_utils.openFileHandles(28, TEST_DIR=TEST_BASE_DIR)
    output_file29, exp_output_file29 = data_gen_utils.openFileHandles(29, TEST_DIR=TEST_BASE_DIR)
    offset = np.max([2, int(dataSize/500)])
    output_file28.write('-- Test for a non-clustered index select followed by an aggregate (control-test, many queries)\n')
    output_file28.write('-- Compare to test 29 for timing differences between B-tree and scan for highly selective queries\n')
    output_file28.write('--\n')
    output_file28.write('-- Query form in SQL:\n')
    output_file28.write('-- SELECT avg(col3) FROM tbl4_ctrl WHERE (col2 >= _ and col2 < _);\n')
    output_file28.write('--\n')
    output_file29.write('-- Test for a non-clustered index select followed by an aggregate (many queries)\n')
    output_file29.write('--\n')
    output_file29.write('-- Query form in SQL:\n')
    output_file29.write('-- SELECT avg(col3) FROM tbl4 WHERE (col2 >= _ and col2 < _);\n')
    output_file29.write('--\n')
    for i in range(100):
        val1 = np.random.randint(0, int((dataSize/5) - offset))
        output_file28.write('s{}=select(db1.tbl4_ctrl.col2,{},{})\n'.format(i, val1, val1 + offset))
        output_file28.write('f{}=fetch(db1.tbl4_ctrl.col3,s{})\n'.format(i,i))
        output_file28.write('a{}=avg(f{})\n'.format(i,i))
        output_file28.write('print(a{})\n'.format(i))
        output_file29.write('s{}=select(db1.tbl4.col2,{},{})\n'.format(i, val1, val1 + offset))
        output_file29.write('f{}=fetch(db1.tbl4.col3,s{})\n'.format(i,i))
        output_file29.write('a{}=avg(f{})\n'.format(i,i))
        output_file29.write('print(a{})\n'.format(i))
        # generate expected results
        dfSelectMask1 = (dataTable['col2'] >= val1) & (dataTable['col2'] < (val1 + offset))
        values = dataTable[dfSelectMask1]['col3']
        mean_result = np.round(values.mean(), PLACES_TO_ROUND)
        if (math.isnan(mean_result)):
            exp_output_file28.write('0.00\n')
            exp_output_file29.write('0.00\n')
        else:
            exp_output_file28.write('{:0.2f}\n'.format(mean_result))
            exp_output_file29.write('{:0.2f}\n'.format(mean_result))
    data_gen_utils.closeFileHandles(output_file28, exp_output_file28)
    data_gen_utils.closeFileHandles(output_file29, exp_output_file29)

def createTest30():
    output_file, exp_output_file = data_gen_utils.openFileHandles(30, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Test for creating table with indexes\n')
    output_file.write('--\n')
    output_file.write('-- Table tbl4_clustered_btree has a clustered index with col3 being the leading column.\n')
    output_file.write('-- The clustered index has the form of a B-Tree.\n')
    output_file.write('-- The table also has a secondary sorted index.\n')
    output_file.write('--\n')
    output_file.write('-- Loads data from: data4_clustered_btree.csv\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl4_clustered_btree",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl4_clustered_btree)\n')
    output_file.write('create(col,"col2",db1.tbl4_clustered_btree)\n')
    output_file.write('create(col,"col3",db1.tbl4_clustered_btree)\n')
    output_file.write('create(col,"col4",db1.tbl4_clustered_btree)\n')
    output_file.write('-- Create a clustered index on col3\n')
    output_file.write('create(idx,db1.tbl4_clustered_btree.col3,btree,clustered)\n')
    output_file.write('-- Create an unclustered btree index on col2\n')
    output_file.write('create(idx,db1.tbl4_clustered_btree.col2,sorted,unclustered)\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately in the form of a clustered index\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/data4_clustered_btree.csv")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data and their indexes are durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTest31(dataTable, dataSize):
    output_file, exp_output_file = data_gen_utils.openFileHandles(31, TEST_DIR=TEST_BASE_DIR)
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    # selectivity = 
    offset = np.max([1, int(dataSize/5000)])
    offset2 = np.max([2, int(dataSize/2500)])
    val1 = np.random.randint(0, int((dataSize/5) - offset))
    val2 = np.random.randint(0, int((dataSize/5) - offset2))
    # generate test 31
    output_file.write('--\n')
    output_file.write('-- tbl4_clustered_btree has a secondary sorted index on col2, and a clustered b-tree index on col3\n')
    output_file.write('-- testing for correctness\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT col1 FROM tbl4_clustered_btree WHERE col3 >= {} and col3 < {};\n'.format(val1, val1+offset))
    output_file.write('-- SELECT col1 FROM tbl4_clustered_btree WHERE col3 >= {} and col3 < {};\n'.format(val2, val2+offset2))
    output_file.write('--\n')
    output_file.write('-- since col3 has a clustered index, the index is expected to be used by the select operator\n')
    output_file.write('s1=select(db1.tbl4_clustered_btree.col3,{},{})\n'.format(val1, val1 + offset))
    output_file.write('f1=fetch(db1.tbl4_clustered_btree.col1,s1)\n')
    output_file.write('print(f1)\n')
    output_file.write('s2=select(db1.tbl4_clustered_btree.col3,{},{})\n'.format(val2, val2 + offset2))
    output_file.write('f2=fetch(db1.tbl4_clustered_btree.col1,s2)\n')
    output_file.write('print(f2)\n')
    # generate expected results
    dfSelectMask1 = (dataTable['col3'] >= val1) & (dataTable['col3'] < (val1 + offset))
    dfSelectMask2 = (dataTable['col3'] >= val2) & (dataTable['col3'] < (val2 + offset2))
    output1 = dataTable[dfSelectMask1]['col1']
    output2 = dataTable[dfSelectMask2]['col1']
    exp_output_file.write(data_gen_utils.outputPrint(output1))
    exp_output_file.write('\n\n')
    exp_output_file.write(data_gen_utils.outputPrint(output2))
    exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTest32(dataTable, dataSize):
    output_file, exp_output_file = data_gen_utils.openFileHandles(32, TEST_DIR=TEST_BASE_DIR)
    offset = np.max([2, int(dataSize/1000)])
    output_file.write('-- Test for a non-clustered index select followed by an aggregate\n')
    output_file.write('--\n')
    output_file.write('-- Query form in SQL:\n')
    output_file.write('-- SELECT sum(col3) FROM tbl4_clustered_btree WHERE (col2 >= _ and col2 < _);\n')
    output_file.write('--\n')
    for i in range(5):
        val1 = np.random.randint(0, int((dataSize/5) - offset))
        output_file.write('s{}=select(db1.tbl4_clustered_btree.col2,{},{})\n'.format(i, val1, val1 + offset))
        output_file.write('f{}=fetch(db1.tbl4_clustered_btree.col3,s{})\n'.format(i,i))
        output_file.write('a{}=sum(f{})\n'.format(i,i))
        output_file.write('print(a{})\n'.format(i))
        # generate expected results
        dfSelectMask1 = (dataTable['col2'] >= val1) & (dataTable['col2'] < (val1 + offset))
        values = dataTable[dfSelectMask1]['col3']
        sum_result = values.sum()
        if (math.isnan(sum_result)):
            exp_output_file.write('0\n')
        else:
            exp_output_file.write(str(sum_result) + '\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTest33To38(dataTable, dataSize):
    table_names=['tbl4_ctrl','tbl4','tbl4_clustered_btree']
    selectivites=['0.1%','1%']
    #selectivity 0.1%
    offset1 = np.max([1, int(dataSize/5000)])
    #selectivity 1%
    offset2 = np.max([2, int(dataSize/500)])
    offsets=[offset1, offset2]
    test_start=33
    for offset, selectivity in zip(offsets, selectivites):
        for table_name in table_names:
            output_file, exp_output_file = data_gen_utils.openFileHandles(test_start, TEST_DIR=TEST_BASE_DIR)

            output_file.write('--\n')
            output_file.write('-- selectivity={}\n'.format(selectivity))
            output_file.write('-- Query in SQL:\n')
            output_file.write('-- SELECT avg(col1) FROM {} WHERE col3 >= _ and col3 < _;\n'.format(table_name))
            output_file.write('--\n')

            for i in range(20):

                val = np.random.randint(0, int((dataSize/5) - offset))

                output_file.write('s{}=select(db1.{}.col3,{},{})\n'.format(i, table_name,val, val + offset))
                output_file.write('f{}=fetch(db1.{}.col1,s{})\n'.format(i, table_name, i))
                output_file.write('a{}=avg(f{})\n'.format(i,i))
                output_file.write('print(a{})\n'.format(i))

                dfSelectMask = (dataTable['col3'] >= val) & (dataTable['col3'] < (val + offset))
                values = dataTable[dfSelectMask]['col1']
                mean_result = np.round(values.mean(), PLACES_TO_ROUND)
                if (math.isnan(mean_result)):
                    exp_output_file.write('0.00\n')
                else:
                    exp_output_file.write('{:0.2f}\n'.format(mean_result))


            data_gen_utils.closeFileHandles(output_file, exp_output_file)
            test_start += 1

def createTest39To44(dataTable, dataSize):
    table_names=['tbl4_ctrl','tbl4','tbl4_clustered_btree']
    selectivites=['0.1%','1%']
    #selectivity 0.1%
    offset1 = np.max([1, int(dataSize/5000)])
    #selectivity 1%
    offset2 = np.max([2, int(dataSize/500)])
    offsets=[offset1, offset2]
    test_start=39
    for offset, selectivity in zip(offsets, selectivites):
        for table_name in table_names:
            output_file, exp_output_file = data_gen_utils.openFileHandles(test_start, TEST_DIR=TEST_BASE_DIR)

            output_file.write('--\n')
            output_file.write('-- selectivity={}\n'.format(selectivity))
            output_file.write('-- Query in SQL:\n')
            output_file.write('-- SELECT avg(col3) FROM {} WHERE col2 >= _ and col2 < _;\n'.format(table_name))
            output_file.write('--\n')

            for i in range(20):

                val = np.random.randint(0, int((dataSize/5) - offset))

                output_file.write('s{}=select(db1.{}.col2,{},{})\n'.format(i, table_name,val, val + offset))
                output_file.write('f{}=fetch(db1.{}.col3,s{})\n'.format(i, table_name, i))
                output_file.write('a{}=avg(f{})\n'.format(i,i))
                output_file.write('print(a{})\n'.format(i))

                dfSelectMask = (dataTable['col2'] >= val) & (dataTable['col2'] < (val + offset))
                values = dataTable[dfSelectMask]['col3']
                mean_result = np.round(values.mean(), PLACES_TO_ROUND)
                if (math.isnan(mean_result)):
                    exp_output_file.write('0.00\n')
                else:
                    exp_output_file.write('{:0.2f}\n'.format(mean_result))

            data_gen_utils.closeFileHandles(output_file, exp_output_file)
            test_start += 1


def generateMilestoneThreeFiles(dataSize, randomSeed=47):
    np.random.seed(randomSeed)
    frequentVal1, frequentVal2, dataTable = generateDataMilestone3(dataSize)  
    createTest20()
    createTest21()
    createTests22And23(dataTable, dataSize)
    createTest24(dataTable, dataSize)
    createTests25And26(dataTable, dataSize)
    createTest27(dataTable, frequentVal1, frequentVal2)
    createTests28And29(dataTable, dataSize)
    createTest30()
    createTest31(dataTable, dataSize)
    createTest32(dataTable, dataSize)

def main(argv):
    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR

    dataSize = int(argv[0])
    if len(argv) > 1:
        randomSeed = int(argv[1])
    else:
        randomSeed = 47

    # override the base directory for where to output test related files
    if len(argv) > 2:
        TEST_BASE_DIR = argv[2]
        if len(argv) > 3:
            DOCKER_TEST_BASE_DIR = argv[3]
    generateMilestoneThreeFiles(dataSize, randomSeed=randomSeed)

if __name__ == "__main__":
    main(sys.argv[1:])

