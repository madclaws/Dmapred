# Dmapred
Distributed MapReduce Systems in Elixir

## Overview
This project is a demo of Distributed Mapreduce systems using Elixir.

- Distributed network is done by in-built distributive primitives of Erlang. 
- Project is inspired by [MIT's 6.824 Lab 1: MapReduce](http://nil.lcs.mit.edu/6.824/2020/labs/lab-mr.html)

----------------

### mapred_seq
A sequential simple mapreduce. Just to give us the idea
of how a basic mapreduce works. Details about running it is in its [readme.md](https://github.com/madclaws/Dmapred/tree/master/mapred_seq#mapredseq).

----------------

### dmapred

A distributed Mapreduce demo. Details about running it is in its [readme.md](https://github.com/madclaws/Dmapred/tree/master/dmapred#dmapred).


## Goal
The output from **mapred_seq** (```mapred_seq/mr-out-0```) should be same as the output from **dmapred**  (```cat outputs/mr-out-* | sort | more```)

----------------


## Dmapred in action

- There is 1 Master and 4 Workers
- Top left window (largest pane) is the Master.
- We starts the master first, and then workers.
- The intermediate files are generated at ``dmapred/intermediates``
- The final files will be generated at ``dmapred/outputs``

![](assets/dmapred.gif)