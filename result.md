## Existing or forseen issues for SFA
    - SFA requires data from all instances; but all DB schemas may not be in sync
    - Memory issues with existing migration scripts

## Completed & Tested:
    - Maintain Instance Level Schema
    - ProcessPoolExecutor for built-in Parallel Process handling (replaces RAY in current implementation)
    - GeneratorExpressions to resolve memory issues
    - PEP Standards


### Pending Items
    - Trivial items like logging, error handling & make it in-line with SRT architecture


### Performance:
    - 52,000,000 X 3
    - 10,000,000 X 3
    - 1,000,000 X 1
    - Time Taken :919 secs
**All files accumulate to 188M of rows which is 11G of data on files.**
**TESTED on DOCKER instance with below configuration**
    - 3.6G of RAM
    - 3 vCPU
    - 1.6G SWAP