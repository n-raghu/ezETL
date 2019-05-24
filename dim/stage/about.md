### Current Scenario

#### For LMS
**Populate Staging** -Talend Package <br>
**Build Vault** -Talend Package <br>
**Purge Data** -Talend Packaage <br>

**All the Talend Packages are scheduled and executed by a Python script**

<br><br>

### YAML & Pandas
#### SINGLE YAML CONFIG TO RUN THE PACKAGE

**Populate Staging** -~~Talend Package~~ Pandas using YAML configuration <br>
**Build Vault -Talend Package** <br>
**Purge Data** -~~Talend Packaage~~ Pandas using YAML configuration <br>


### Advantages
| Pandas | Talend |
| ------ | ------ |
| Single YAML File to control both LMS and PHISHPROOF Tables needed to stage | Dynamic control is tedious with open studio |
| Extensible to maintain different versions of Table using Document Model | There could be a workaround but not as easy as we think |
| One time developer efforts | Requires developer efforts if requirement changes. Making it fully dynamic makes the package very complex |
| Parallelism can be implemented on both Instance and Tables structure | Either on Instance or on Table structure but not both |
| Can be exposed to control through UI | Depends on how dynamic we implement the solution |
| For the same amount of developer efforts fully dynamic is possible | Requires more developer and testing efforts to make it fully dynamic

### Things to Remember
<p>
	1. Data structure and Data model is intact <br>
	2. Only changes on the method how staging is populated <br>
	3. We are combining the power of Pandas and Talend to get a better solution <br>
	4. We did not increase any of the technical stack i.e. We already decided to use python for scheduling and execution, and now we increase the scope to populate staging. <br>
	5. Same YAML can be used to reporting database connection settings to Talend
</p>
