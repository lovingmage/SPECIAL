## SPECIAL: Synopsis Assisted Secure Collaborative Analytics

### Dependencies
1. [EMPtool](https://github.com/emp-toolkit/emp-tool)
2. [Emp-sh2pc](https://github.com/emp-toolkit/emp-sh2pc)


### Compilation
After you have installed dependencies, switch to the root directory of SPECIAL and run:

`cmake . & make`

### Test
* IF you want to test the code, type

   `./bin/[binaries] [port number] 1 & ./bin/[binaries] [port number] 2`

* Examples: test filter operator, type

	`./bin/test_filter 1 12345 & ./bin/test_filter 2 12345`

	
### Question
Please send email to cw166@iu.edu
