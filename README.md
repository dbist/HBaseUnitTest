## Overview of HBase Unit Testing methodologies
##### Junit - test for expected values in Puts, Gets, Deletes, etc.
##### Mockito - Mockito is a mocking framework. It goes further than JUnit by allowing you to test the interactions between objects without having to replicate the entire environment
##### MRUnit - library that allows you to unit-test MapReduce jobs (requires to specify classifier in pom, <classifier>hadoop2</classifier>) does not work yet
##### HBaseTestingUtility - makes it easy to write integration tests using a mini-cluster 
