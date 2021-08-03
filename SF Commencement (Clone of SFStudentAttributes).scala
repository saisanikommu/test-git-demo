// Databricks notebook source
// DBTITLE 1,Get Oracle connection details
// MAGIC %run DEV/connectMe

// COMMAND ----------

val url = readDBProperties("salesforceETL")
val url1 = readDBProperties("usersResearch")
val spring_salesforce = "com.springml.spark.salesforce"
val soql_version = "51.0"

// COMMAND ----------

val driver = "oracle.jdbc.driver.OracleDriver"
val fetchSize = "10000"
val propProcessing = new java.util.Properties
propProcessing.setProperty("user",dbutils.secrets.get(scope = "ir_salesforce_etl", key = "userName"))
propProcessing.setProperty("password",dbutils.secrets.get(scope = "ir_salesforce_etl", key = "password")) 
propProcessing.setProperty("driver", driver)

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types.{DateType}
import java.sql.Date
import org.apache.spark.sql.functions._
import java.util.Calendar
import java.text.SimpleDateFormat
val path = "s3a://iredw/wgubi/stg_db_bi_c_commencement"  ///mnt/iredw/users/jaswanth.sanikommu/ stg_db_bi_c_commencement

// COMMAND ----------

import oracle.jdbc.pool.OracleDataSource

var last_modified_date_3=""
var last_modified_date_4=""
var last_modified_date_1 = ""
var last_modified_date_2=""

// COMMAND ----------

// DBTITLE 1,Get the last modified date from the Log table
//val df_student = spark.read.format("jdbc").options(Map( "driver"->driver, "url" -> url, "dbtable"-> "(Select TO_CHAR(max(LASTMODIFIEDDATE),'YYYY-MM-DD hh24:mm:ss') AS LASTMODIFIEDDATELD from wgubisalesforce.BI_C_COMMENCEMENT)","fetchSize" -> fetchSize)).load()
//val last_modified_date = df_student.first.getString(0)


val df_student = spark.read.format("jdbc").options(Map( "driver"->driver, "url" -> url, "dbtable"-> "(select max(LASTMODIFIEDDATE)- INTERVAL '600' SECOND AS LASTMODIFIEDDATELD from wgubisalesforce.DB_SALESFORCE_LOG where table_name ='BI_C_COMMENCEMENT__C')","fetchSize" -> fetchSize)).load()
 
//val last_modified_date = "2017-03-23 15:23:30.0"

val last_modified_date = df_student.first.getTimestamp(0).toString
println("Last modified date is : "+last_modified_date)
 last_modified_date_1 = last_modified_date.substring(0,10)

 last_modified_date_2 = last_modified_date.substring(11,19)

 last_modified_date_3 = last_modified_date_1 + 'T' + last_modified_date_2 + 'Z' + ')'

val LASTMODIFIEDDATE = last_modified_date_1 + 'T' + last_modified_date_2 + 'Z'
val LASTMODIFIEDDATE_validation = last_modified_date_1 + ' ' + last_modified_date_2 


// COMMAND ----------

// DBTITLE 1,Creating Dynamic SOQL 
last_modified_date_4 = """SELECT AccommodationsFor__c,AccommodationsNeeded__c,AccommodationsType__c,AdditionalComments__c,AdditionalDegreeTimeframe__c,AdditionalDegree__c,AddressCanadaState__c,AddressCity__c,AddressCountry__c,AddressCounty__c,AddressLine1__c,AddressLine2__c,AddressNonUSState__c,AddressState__c,AddressUSState__c,AddressZip__c,CeremonyContactAddress__c,CeremonyContactEmail__c,CeremonyContactPhone__c,CeremonyLocale__c,CeremonyPeriod__c,CeremonyStatus__c,CeremonyYear__c,ConnectionReceivedId,ConnectionSentId,CreatedById,CreatedDate,DegreeCollege__c,DegreeProgramCode__c,DegreeProgramName__c,DegreeType__c,EligibilityStatus__c,Exemptions__c,FirstWGUDegree__c,FormStatus__c,GraduationDate__c,GraduationProgramCode__c,GraduationRecord__c,Graduation_Date_Student_Process__c,GuestCount__c,Id,IsDeleted,LastActivityDate,LastModifiedById,LastModifiedDate,LastReferencedDate,LastViewedDate,LocationAppeals__c,MailingCity__c,MailingCountry__c,MailingStateProvince__c,MailingStreet__c,MailingZipPostalCode__c,MediaPermission__c,MilitaryCord__c,MilitaryServices__c,Name,OwnerId,PhotographyPermission__c,ProgramBookManualAdd__c,ProgramBookOptOut__c,Pronunciation__c,ProvidedStudentID__c,RecordTypeId,RemainingCUs__c,ReservedSeatingName__c,RSVPAcceptTexts__c,RSVPAddress__c,RSVPAlumniCelebrationChildCount__c,RSVPAlumniCelebrationChildrenAttending__c,RSVPAlumniCelebrationGuestCount__c,RSVPAttendingAlumniCelebration__c,RSVPCelebrationDegree__c,RSVPCollege__c,RSVPContinuingGrad__c,RSVPDateFirstDegree__c,RSVPDegree__c,RSVPGradAccommodations__c,RSVPGradASL__c,RSVPGradOtherDetails__c,RSVPGradOther__c,RSVPGradServiceAnimal__c,RSVPGradWheelchair__c,RSVPGuestAccommodations__c,RSVPGuestASL__c,RSVPGuestorGradAccommodations__c,RSVPGuestOtherDetails__c,RSVPGuestOther__c,RSVPGuestServiceAnimal__c,RSVPGuestWheelchair__c,RSVPMentorName__c,RSVPName__c,RSVPOtherFamilyWalkingName__c,RSVPOtherFamilyWalking__c,RSVPPhotographyDisclaimer__c,RSVPReasonNotAttending__c,RSVPReporters__c,RSVPShareStory__c,RSVPStudentStory__c,RSVPWhatWasFirstDegree__c,RSVP_First_Name__c,RSVP_Last_Name__c,StudentId__c,StudentMentorEmail__c,StudentMentor__c,StudentStatus__c,Student__c,SystemModstamp,TempID__c,VehicleCount__c,WalkingDegreeType__c,WalkinginCeremony__c,Walking_Degree__c,WhyWGU__c FROM Commencement__c Where  (LastModifiedDate > """
val commencement_soql = last_modified_date_4 + last_modified_date_3

// COMMAND ----------

// DBTITLE 1,Getting data from salesforce
var commencement_soql_df = spark.
                read.
                format(spring_salesforce).
                option("username", dbutils.secrets.get(scope = "ir_salesforce_etl", key = "userNameSF")).
                option("password", dbutils.secrets.get(scope = "ir_salesforce_etl", key = "passwordSF")).
                option("soql", commencement_soql).
                option("version",soql_version).
                option("queryAll", "true").
                load()

// COMMAND ----------

//commencement_soql_df.count //30083 Matching 

// COMMAND ----------

if(commencement_soql_df.count() == 0){
  print("Exiting Notebook without error")
  dbutils.notebook.exit("Success")}

// COMMAND ----------

for (x <- commencement_soql_df.columns) {
     commencement_soql_df = commencement_soql_df.withColumn(x,regexp_replace(col(x),"null",""))
    }

// COMMAND ----------

// DBTITLE 1,Bool function to change True or false to 0 or 1
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import scala.collection.mutable
  import  spark.implicits._
def Bool(samp:DataFrame): DataFrame = {

  var df = samp
  var col =  mutable.MutableList[String]()
  for (x <- df.columns){
    var unq = df.select(x).distinct().collect().map(_(0)).toList
    if ((unq(0) == "true") || (unq(0) == "false")){
      
      col+=x
      
    }
  }
  val booleanToInt = udf {(value: String) => 
  if(value == "false") 0 else if(value == "") 0 else 1
  }
  for (i<-col){
  df = df.withColumn(i,booleanToInt(df(i)))
  }
  
  return df
  
}

commencement_soql_df = Bool(commencement_soql_df)


// COMMAND ----------

// DBTITLE 1,Add the partition field in the dataframe
commencement_soql_df = commencement_soql_df.
withColumn("Created_YearMonth", concat(year($"CREATEDDATE"),format_string("%02d",month($"CREATEDDATE"))))

// COMMAND ----------

commencement_soql_df.createOrReplaceTempView("tempDF1")

// COMMAND ----------

// DBTITLE 1,Explicitly convert datatypes of all columns to match the STG table definition
var commencement_soql_df = spark.sql( s"""select 
cast(AccommodationsFor__c as string) AccommodationsFor__c,
cast(AccommodationsNeeded__c as string) AccommodationsNeeded__c,
cast(AccommodationsType__c as string) AccommodationsType__c,
cast(AdditionalComments__c as string) AdditionalComments__c,
cast(AdditionalDegreeTimeframe__c as string) AdditionalDegreeTimeframe__c,
cast(AdditionalDegree__c as string) AdditionalDegree__c,
cast(AddressCanadaState__c as string) AddressCanadaState__c,
cast(AddressCity__c as string) AddressCity__c,
cast(AddressCountry__c as string) AddressCountry__c,
cast(AddressCounty__c as string) AddressCounty__c,
cast(AddressLine1__c as string) AddressLine1__c,
cast(AddressLine2__c as string) AddressLine2__c,
cast(AddressNonUSState__c as string) AddressNonUSState__c,
cast(AddressState__c as string) AddressState__c,
cast(AddressUSState__c as string) AddressUSState__c,
cast(AddressZip__c as string) AddressZip__c,
cast(CeremonyContactAddress__c as string) CeremonyContactAddress__c,
cast(CeremonyContactEmail__c as string) CeremonyContactEmail__c,
cast(CeremonyContactPhone__c as string) CeremonyContactPhone__c,
cast(CeremonyLocale__c as string) CeremonyLocale__c,
cast(CeremonyPeriod__c as string) CeremonyPeriod__c,
cast(CeremonyStatus__c as string) CeremonyStatus__c,
cast(CeremonyYear__c as string) CeremonyYear__c,
cast(ConnectionReceivedId as string) ConnectionReceivedId,
cast(ConnectionSentId as string) ConnectionSentId,
cast(CreatedById as string) CreatedById,
cast(CreatedDate as string) CreatedDate,
cast(DegreeCollege__c as string) DegreeCollege__c,
cast(DegreeProgramCode__c as string) DegreeProgramCode__c,
cast(DegreeProgramName__c as string) DegreeProgramName__c,
cast(DegreeType__c as string) DegreeType__c,
cast(EligibilityStatus__c as string) EligibilityStatus__c,
cast(Exemptions__c as string) Exemptions__c,
cast(FirstWGUDegree__c as string) FirstWGUDegree__c,
cast(FormStatus__c as string) FormStatus__c,
cast(GraduationDate__c as string) GraduationDate__c,
cast(GraduationProgramCode__c as string) GraduationProgramCode__c,
cast(GraduationRecord__c as string) GraduationRecord__c,
cast(Graduation_Date_Student_Process__c as string) Graduation_Date_Student_Process__c,
cast(GuestCount__c as string) GuestCount__c,
cast(Id as string) Id,
cast(IsDeleted as string) IsDeleted,
cast(LastActivityDate as string) LastActivityDate,
cast(LastModifiedById as string) LastModifiedById,
cast(LastModifiedDate as string) LastModifiedDate,
cast(LastReferencedDate as string) LastReferencedDate,
cast(LastViewedDate as string) LastViewedDate,
cast(LocationAppeals__c as string) LocationAppeals__c,
cast(MailingCity__c as string) MailingCity__c,
cast(MailingCountry__c as string) MailingCountry__c,
cast(MailingStateProvince__c as string) MailingStateProvince__c,
cast(MailingStreet__c as string) MailingStreet__c,
cast(MailingZipPostalCode__c as string) MailingZipPostalCode__c,
cast(MediaPermission__c as string) MediaPermission__c,
cast(MilitaryCord__c as string) MilitaryCord__c,
cast(MilitaryServices__c as string) MilitaryServices__c,
cast(Name as string) Name,
cast(OwnerId as string) OwnerId,
cast(PhotographyPermission__c as string) PhotographyPermission__c,
cast(ProgramBookManualAdd__c as string) ProgramBookManualAdd__c,
cast(ProgramBookOptOut__c as string) ProgramBookOptOut__c,
cast(Pronunciation__c as string) Pronunciation__c,
cast(ProvidedStudentID__c as string) ProvidedStudentID__c,
cast(RecordTypeId as string) RecordTypeId,
cast(RemainingCUs__c as string) RemainingCUs__c,
cast(ReservedSeatingName__c as string) ReservedSeatingName__c,
cast(RSVPAcceptTexts__c as string) RSVPAcceptTexts__c,
cast(RSVPAddress__c as string) RSVPAddress__c,
cast(RSVPAlumniCelebrationChildCount__c as string) RSVPAlumniCelebrationChildCount__c,
cast(RSVPAlumniCelebrationChildrenAttending__c as string) RSVPAlumniCelebrationChildrenAttending__c,
cast(RSVPAlumniCelebrationGuestCount__c as string) RSVPAlumniCelebrationGuestCount__c,
cast(RSVPAttendingAlumniCelebration__c as string) RSVPAttendingAlumniCelebration__c,
cast(RSVPCelebrationDegree__c as string) RSVPCelebrationDegree__c,
cast(RSVPCollege__c as string) RSVPCollege__c,
cast(RSVPContinuingGrad__c as string) RSVPContinuingGrad__c,
cast(RSVPDateFirstDegree__c as string) RSVPDateFirstDegree__c,
cast(RSVPDegree__c as string) RSVPDegree__c,
cast(RSVPGradAccommodations__c as string) RSVPGradAccommodations__c,
cast(RSVPGradASL__c as string) RSVPGradASL__c,
cast(RSVPGradOtherDetails__c as string) RSVPGradOtherDetails__c,
cast(RSVPGradOther__c as string) RSVPGradOther__c,
cast(RSVPGradServiceAnimal__c as string) RSVPGradServiceAnimal__c,
cast(RSVPGradWheelchair__c as string) RSVPGradWheelchair__c,
cast(RSVPGuestAccommodations__c as string) RSVPGuestAccommodations__c,
cast(RSVPGuestASL__c as string) RSVPGuestASL__c,
cast(RSVPGuestorGradAccommodations__c as string) RSVPGuestorGradAccommodations__c,
cast(RSVPGuestOtherDetails__c as string) RSVPGuestOtherDetails__c,
cast(RSVPGuestOther__c as string) RSVPGuestOther__c,
cast(RSVPGuestServiceAnimal__c as string) RSVPGuestServiceAnimal__c,
cast(RSVPGuestWheelchair__c as string) RSVPGuestWheelchair__c,
cast(RSVPMentorName__c as string) RSVPMentorName__c,
cast(RSVPName__c as string) RSVPName__c,
cast(RSVPOtherFamilyWalkingName__c as string) RSVPOtherFamilyWalkingName__c,
cast(RSVPOtherFamilyWalking__c as string) RSVPOtherFamilyWalking__c,
cast(RSVPPhotographyDisclaimer__c as string) RSVPPhotographyDisclaimer__c,
cast(RSVPReasonNotAttending__c as string) RSVPReasonNotAttending__c,
cast(RSVPReporters__c as string) RSVPReporters__c,
cast(RSVPShareStory__c as string) RSVPShareStory__c,
cast(RSVPStudentStory__c as string) RSVPStudentStory__c,
cast(RSVPWhatWasFirstDegree__c as string) RSVPWhatWasFirstDegree__c,
cast(RSVP_First_Name__c as string) RSVP_First_Name__c,
cast(RSVP_Last_Name__c as string) RSVP_Last_Name__c,
cast(StudentId__c as string) StudentId__c,
cast(StudentMentorEmail__c as string) StudentMentorEmail__c,
cast(StudentMentor__c as string) StudentMentor__c,
cast(StudentStatus__c as string) StudentStatus__c,
cast(Student__c as string) Student__c,
cast(SystemModstamp as string) SystemModstamp,
cast(TempID__c as string) TempID__c,
cast(VehicleCount__c as string) VehicleCount__c,
cast(WalkingDegreeType__c as string) WalkingDegreeType__c,
cast(WalkinginCeremony__c as string) WalkinginCeremony__c,
cast(Walking_Degree__c as string) Walking_Degree__c,
cast(WhyWGU__c as string) WhyWGU__c,
cast(Created_YearMonth as string) Created_YearMonth
from tempDF1""")

commencement_soql_df.createOrReplaceTempView("commencement_soql_df")

// COMMAND ----------

print(commencement_soql_df.schema)

// COMMAND ----------

// DBTITLE 1,Create the stage delta table
commencement_soql_df.write.mode("overwrite").format("delta").partitionBy("Created_YearMonth").option("path", path).saveAsTable("wgubisalesforce.stg_db_commencement")  

// COMMAND ----------

// MAGIC %sql
// MAGIC --show create table wgubisalesforce.stg_db_commencement
// MAGIC /*CREATE TABLE `wgubisalesforce`.`BI_C_COMMENCEMENT` (`AccommodationsFor__c` STRING, `AccommodationsNeeded__c` STRING, `AccommodationsType__c` STRING, `AdditionalComments__c` STRING, `AdditionalDegreeTimeframe__c` STRING, `AdditionalDegree__c` STRING, `AddressCanadaState__c` STRING, `AddressCity__c` STRING, `AddressCountry__c` STRING, `AddressCounty__c` STRING, `AddressLine1__c` STRING, `AddressLine2__c` STRING, `AddressNonUSState__c` STRING, `AddressState__c` STRING, `AddressUSState__c` STRING, `AddressZip__c` STRING, `CeremonyContactAddress__c` STRING, `CeremonyContactEmail__c` STRING, `CeremonyContactPhone__c` STRING, `CeremonyLocale__c` STRING, `CeremonyPeriod__c` STRING, `CeremonyStatus__c` STRING, `CeremonyYear__c` STRING, `ConnectionReceivedId` STRING, `ConnectionSentId` STRING, `CreatedById` STRING, `CreatedDate` STRING, `DegreeCollege__c` STRING, `DegreeProgramCode__c` STRING, `DegreeProgramName__c` STRING, `DegreeType__c` STRING, `EligibilityStatus__c` STRING, `Exemptions__c` STRING, `FirstWGUDegree__c` STRING, `FormStatus__c` STRING, `GraduationDate__c` STRING, `GraduationProgramCode__c` STRING, `GraduationRecord__c` STRING, `Graduation_Date_Student_Process__c` STRING, `GuestCount__c` STRING, `Id` STRING, `IsDeleted` STRING, `LastActivityDate` STRING, `LastModifiedById` STRING, `LastModifiedDate` STRING, `LastReferencedDate` STRING, `LastViewedDate` STRING, `LocationAppeals__c` STRING, `MailingCity__c` STRING, `MailingCountry__c` STRING, `MailingStateProvince__c` STRING, `MailingStreet__c` STRING, `MailingZipPostalCode__c` STRING, `MediaPermission__c` STRING, `MilitaryCord__c` STRING, `MilitaryServices__c` STRING, `Name` STRING, `OwnerId` STRING, `PhotographyPermission__c` STRING, `ProgramBookManualAdd__c` STRING, `ProgramBookOptOut__c` STRING, `Pronunciation__c` STRING, `ProvidedStudentID__c` STRING, `RecordTypeId` STRING, `RemainingCUs__c` STRING, `ReservedSeatingName__c` STRING, `RSVPAcceptTexts__c` STRING, `RSVPAddress__c` STRING, `RSVPAlumniCelebrationChildCount__c` STRING, `RSVPAlumniCelebrationChildrenAttending__c` STRING, `RSVPAlumniCelebrationGuestCount__c` STRING, `RSVPAttendingAlumniCelebration__c` STRING, `RSVPCelebrationDegree__c` STRING, `RSVPCollege__c` STRING, `RSVPContinuingGrad__c` STRING, `RSVPDateFirstDegree__c` STRING, `RSVPDegree__c` STRING, `RSVPGradAccommodations__c` STRING, `RSVPGradASL__c` STRING, `RSVPGradOtherDetails__c` STRING, `RSVPGradOther__c` STRING, `RSVPGradServiceAnimal__c` STRING, `RSVPGradWheelchair__c` STRING, `RSVPGuestAccommodations__c` STRING, `RSVPGuestASL__c` STRING, `RSVPGuestorGradAccommodations__c` STRING, `RSVPGuestOtherDetails__c` STRING, `RSVPGuestOther__c` STRING, `RSVPGuestServiceAnimal__c` STRING, `RSVPGuestWheelchair__c` STRING, `RSVPMentorName__c` STRING, `RSVPName__c` STRING, `RSVPOtherFamilyWalkingName__c` STRING, `RSVPOtherFamilyWalking__c` STRING, `RSVPPhotographyDisclaimer__c` STRING, `RSVPReasonNotAttending__c` STRING, `RSVPReporters__c` STRING, `RSVPShareStory__c` STRING, `RSVPStudentStory__c` STRING, `RSVPWhatWasFirstDegree__c` STRING, `RSVP_First_Name__c` STRING, `RSVP_Last_Name__c` STRING, `StudentId__c` STRING, `StudentMentorEmail__c` STRING, `StudentMentor__c` STRING, `StudentStatus__c` STRING, `Student__c` STRING, `SystemModstamp` STRING, `TempID__c` STRING, `VehicleCount__c` STRING, `WalkingDegreeType__c` STRING, `WalkinginCeremony__c` STRING, `Walking_Degree__c` STRING, `WhyWGU__c` STRING, `Created_YearMonth` STRING)
// MAGIC USING delta
// MAGIC OPTIONS (
// MAGIC   path 's3a://iredw/wgubi/bi_c_commencement'
// MAGIC )
// MAGIC PARTITIONED BY (Created_YearMonth)
// MAGIC */

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE wgubisalesforce.stg_db_commencement

// COMMAND ----------

// DBTITLE 1,Merge sql function
def dySql(colist: List[String],t1:String,t2:String): String = {
      import scala.collection.mutable
      var matched = new mutable.MutableList[String]()
      var notmatched = new mutable.MutableList[String]()
      for(x<-colist){
        var new_st = "b."+x.toString+"="+"d."+x.toString
        matched += new_st
      }
      for(x<-colist){
        var new_st = "d."+x.toString
        notmatched += new_st
      }
      var sql = "MERGE INTO "+t1+" b USING "+t2+" d ON" +
        " (b.ID = d.ID and b.Created_YearMonth=d.Created_YearMonth)  WHEN MATCHED THEN UPDATE SET "+matched.mkString(",")+
      " WHEN NOT MATCHED THEN INSERT("+colist.mkString(",")+") values ("+notmatched.mkString(",")+")"

      return sql
    }


// COMMAND ----------

// DBTITLE 1,Statement for merging stg to Main table
val sql = dySql(commencement_soql_df.columns.toSeq.toList,"wgubisalesforce.BI_C_COMMENCEMENT","wgubisalesforce.stg_db_commencement")

// COMMAND ----------

// DBTITLE 1,Executing merge from stg to Main table
spark.sql(sql)

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE wgubisalesforce.BI_C_COMMENCEMENT

// COMMAND ----------

// DBTITLE 1,Get the list of IDs to be Updated
var update_Oracle = spark.sql(s"""select *, ROW_NUMBER() OVER (ORDER BY ID DESC) AS rowNum                
 from wgubisalesforce.stg_db_commencement """)  
update_Oracle.createOrReplaceTempView("DeleteOracle")

// COMMAND ----------

// DBTITLE 1,Inserting into Oracle stg and merging that stg with Main table in Oracle
import oracle.jdbc.pool.OracleDataSource
val ods = new OracleDataSource()
ods.setUser(dbutils.secrets.get(scope = "ir_salesforce_etl", key = "userName"))
ods.setPassword(dbutils.secrets.get(scope = "ir_salesforce_etl", key = "password"))
ods.setURL(url)
val con = ods.getConnection()
println("Connected")
val incremental_value =  10000
var j = incremental_value
val maxRowNum = spark.sql("select max(rowNum) from DeleteOracle").first.getInt(0)
println("Number of rows to be merged"+maxRowNum)
for(i <-0 to maxRowNum by incremental_value)
{
  println("I value  ==="+i+" : J value === "+j)
  val insert_temp = spark.sql(s"""select AccommodationsFor__c,AccommodationsNeeded__c,AccommodationsType__c,AdditionalComments__c,AdditionalDegreeTimeframe__c,AdditionalDegree__c,AddressCanadaState__c,AddressCity__c,AddressCountry__c,AddressCounty__c,AddressLine1__c,AddressLine2__c,AddressNonUSState__c,AddressState__c,AddressUSState__c,AddressZip__c,CeremonyContactAddress__c,CeremonyContactEmail__c,CeremonyContactPhone__c,CeremonyLocale__c,CeremonyPeriod__c,CeremonyStatus__c,CeremonyYear__c,ConnectionReceivedId,ConnectionSentId,CreatedById,cast(CreatedDate as timestamp) as CreatedDate,DegreeCollege__c,DegreeProgramCode__c,DegreeProgramName__c,DegreeType__c,EligibilityStatus__c,Exemptions__c,FirstWGUDegree__c,FormStatus__c,GraduationDate__c,GraduationProgramCode__c,GraduationRecord__c,Graduation_Date_Student_Process__c AS Grad_Date_Student_Process ,GuestCount__c,Id,IsDeleted,LastActivityDate,LastModifiedById,cast(LastModifiedDate as timestamp) as LastModifiedDate,LastReferencedDate,LastViewedDate,LocationAppeals__c,MailingCity__c,MailingCountry__c,MailingStateProvince__c,MailingStreet__c,MailingZipPostalCode__c,MediaPermission__c,MilitaryCord__c,MilitaryServices__c,Name,OwnerId,PhotographyPermission__c,ProgramBookManualAdd__c,ProgramBookOptOut__c,Pronunciation__c,ProvidedStudentID__c,RecordTypeId,RemainingCUs__c,ReservedSeatingName__c,RSVPAcceptTexts__c,RSVPAddress__c,RSVPAlumniCelebrationChildCount__c AS RSVPAlumniClbratnChildCnt,RSVPAlumniCelebrationChildrenAttending__c AS RSVPAlumniClbratnChildAtnding,RSVPAlumniCelebrationGuestCount__c AS RSVPAlumniCelebratnGuestCount,RSVPAttendingAlumniCelebration__c AS RSVPAttendingAlumniClbratn,RSVPCelebrationDegree__c,RSVPCollege__c,RSVPContinuingGrad__c,RSVPDateFirstDegree__c,RSVPDegree__c,RSVPGradAccommodations__c,RSVPGradASL__c,RSVPGradOtherDetails__c,RSVPGradOther__c,RSVPGradServiceAnimal__c,RSVPGradWheelchair__c,RSVPGuestAccommodations__c,RSVPGuestASL__c,RSVPGuestorGradAccommodations__c AS RSVPGuestorGradAcomodation,RSVPGuestOtherDetails__c,RSVPGuestOther__c,RSVPGuestServiceAnimal__c,RSVPGuestWheelchair__c,RSVPMentorName__c,RSVPName__c,RSVPOtherFamilyWalkingName__c AS RSVPOtherFamilyWalkingName,RSVPOtherFamilyWalking__c,RSVPPhotographyDisclaimer__c,RSVPReasonNotAttending__c,RSVPReporters__c,RSVPShareStory__c,RSVPStudentStory__c,RSVPWhatWasFirstDegree__c,RSVP_First_Name__c,RSVP_Last_Name__c,StudentId__c,StudentMentorEmail__c,StudentMentor__c,StudentStatus__c,Student__c,cast(SystemModstamp as timestamp) as SystemModstamp,TempID__c,VehicleCount__c,WalkingDegreeType__c,WalkinginCeremony__c,Walking_Degree__c,WhyWGU__c,Created_YearMonth from DeleteOracle where rowNum > '$i' and rowNum <= '$j' """)
  insert_temp.write.mode("Overwrite").options(option_write).format("jdbc").save()
  insert_temp.createOrReplaceTempView("insert_temp")  
  val num_insert = spark.sql("select count(*) from insert_temp").first.getLong(0)
  println("Merged record "+num_insert)
  var merge_Oracle = """MERGE INTO WGUBISALESFORCE.BI_C_COMMENCEMENT__C b USING WGUBISALESFORCE.STG_DB_COMMENCEMENT__C d ON (b.ID = d.ID)  WHEN MATCHED THEN UPDATE SET b.AccommodationsFor__c=d.AccommodationsFor__c,b.AccommodationsNeeded__c=d.AccommodationsNeeded__c,b.AccommodationsType__c=d.AccommodationsType__c,b.AdditionalComments__c=d.AdditionalComments__c,b.AdditionalDegreeTimeframe__c=d.AdditionalDegreeTimeframe__c,b.AdditionalDegree__c=d.AdditionalDegree__c,b.AddressCanadaState__c=d.AddressCanadaState__c,b.AddressCity__c=d.AddressCity__c,b.AddressCountry__c=d.AddressCountry__c,b.AddressCounty__c=d.AddressCounty__c,b.AddressLine1__c=d.AddressLine1__c,b.AddressLine2__c=d.AddressLine2__c,b.AddressNonUSState__c=d.AddressNonUSState__c,b.AddressState__c=d.AddressState__c,b.AddressUSState__c=d.AddressUSState__c,b.AddressZip__c=d.AddressZip__c,b.CeremonyContactAddress__c=d.CeremonyContactAddress__c,b.CeremonyContactEmail__c=d.CeremonyContactEmail__c,b.CeremonyContactPhone__c=d.CeremonyContactPhone__c,b.CeremonyLocale__c=d.CeremonyLocale__c,b.CeremonyPeriod__c=d.CeremonyPeriod__c,b.CeremonyStatus__c=d.CeremonyStatus__c,b.CeremonyYear__c=d.CeremonyYear__c,b.ConnectionReceivedId=d.ConnectionReceivedId,b.ConnectionSentId=d.ConnectionSentId,b.CreatedById=d.CreatedById,b.CreatedDate=d.CreatedDate,b.DegreeCollege__c=d.DegreeCollege__c,b.DegreeProgramCode__c=d.DegreeProgramCode__c,b.DegreeProgramName__c=d.DegreeProgramName__c,b.DegreeType__c=d.DegreeType__c,b.EligibilityStatus__c=d.EligibilityStatus__c,b.Exemptions__c=d.Exemptions__c,b.FirstWGUDegree__c=d.FirstWGUDegree__c,b.FormStatus__c=d.FormStatus__c,b.GraduationDate__c=d.GraduationDate__c,b.GraduationProgramCode__c=d.GraduationProgramCode__c,b.GraduationRecord__c=d.GraduationRecord__c,b.Grad_Date_Student_Process=d.Grad_Date_Student_Process,b.GuestCount__c=d.GuestCount__c,b.IsDeleted=d.IsDeleted,b.LastActivityDate=d.LastActivityDate,b.LastModifiedById=d.LastModifiedById,b.LastModifiedDate=d.LastModifiedDate,b.LastReferencedDate=d.LastReferencedDate,b.LastViewedDate=d.LastViewedDate,b.LocationAppeals__c=d.LocationAppeals__c,b.MailingCity__c=d.MailingCity__c,b.MailingCountry__c=d.MailingCountry__c,b.MailingStateProvince__c=d.MailingStateProvince__c,b.MailingStreet__c=d.MailingStreet__c,b.MailingZipPostalCode__c=d.MailingZipPostalCode__c,b.MediaPermission__c=d.MediaPermission__c,b.MilitaryCord__c=d.MilitaryCord__c,b.MilitaryServices__c=d.MilitaryServices__c,b.Name=d.Name,b.OwnerId=d.OwnerId,b.PhotographyPermission__c=d.PhotographyPermission__c,b.ProgramBookManualAdd__c=d.ProgramBookManualAdd__c,b.ProgramBookOptOut__c=d.ProgramBookOptOut__c,b.Pronunciation__c=d.Pronunciation__c,b.ProvidedStudentID__c=d.ProvidedStudentID__c,b.RecordTypeId=d.RecordTypeId,b.RemainingCUs__c=d.RemainingCUs__c,b.ReservedSeatingName__c=d.ReservedSeatingName__c,b.RSVPAcceptTexts__c=d.RSVPAcceptTexts__c,b.RSVPAddress__c=d.RSVPAddress__c,b.RSVPAlumniClbratnChildCnt=d.RSVPAlumniClbratnChildCnt,b.RSVPAlumniClbratnChildAtnding=d.RSVPAlumniClbratnChildAtnding,b.RSVPAlumniCelebratnGuestCount=d.RSVPAlumniCelebratnGuestCount,b.RSVPAttendingAlumniClbratn=d.RSVPAttendingAlumniClbratn,b.RSVPCelebrationDegree__c=d.RSVPCelebrationDegree__c,b.RSVPCollege__c=d.RSVPCollege__c,b.RSVPContinuingGrad__c=d.RSVPContinuingGrad__c,b.RSVPDateFirstDegree__c=d.RSVPDateFirstDegree__c,b.RSVPDegree__c=d.RSVPDegree__c,b.RSVPGradAccommodations__c=d.RSVPGradAccommodations__c,b.RSVPGradASL__c=d.RSVPGradASL__c,b.RSVPGradOtherDetails__c=d.RSVPGradOtherDetails__c,b.RSVPGradOther__c=d.RSVPGradOther__c,b.RSVPGradServiceAnimal__c=d.RSVPGradServiceAnimal__c,b.RSVPGradWheelchair__c=d.RSVPGradWheelchair__c,b.RSVPGuestAccommodations__c=d.RSVPGuestAccommodations__c,b.RSVPGuestASL__c=d.RSVPGuestASL__c,b.RSVPGuestorGradAcomodation=d.RSVPGuestorGradAcomodation,b.RSVPGuestOtherDetails__c=d.RSVPGuestOtherDetails__c,b.RSVPGuestOther__c=d.RSVPGuestOther__c,b.RSVPGuestServiceAnimal__c=d.RSVPGuestServiceAnimal__c,b.RSVPGuestWheelchair__c=d.RSVPGuestWheelchair__c,b.RSVPMentorName__c=d.RSVPMentorName__c,b.RSVPName__c=d.RSVPName__c,b.RSVPOtherFamilyWalkingName=d.RSVPOtherFamilyWalkingName,b.RSVPOtherFamilyWalking__c=d.RSVPOtherFamilyWalking__c,b.RSVPPhotographyDisclaimer__c=d.RSVPPhotographyDisclaimer__c,b.RSVPReasonNotAttending__c=d.RSVPReasonNotAttending__c,b.RSVPReporters__c=d.RSVPReporters__c,b.RSVPShareStory__c=d.RSVPShareStory__c,b.RSVPStudentStory__c=d.RSVPStudentStory__c,b.RSVPWhatWasFirstDegree__c=d.RSVPWhatWasFirstDegree__c,b.RSVP_First_Name__c=d.RSVP_First_Name__c,b.RSVP_Last_Name__c=d.RSVP_Last_Name__c,b.StudentId__c=d.StudentId__c,b.StudentMentorEmail__c=d.StudentMentorEmail__c,b.StudentMentor__c=d.StudentMentor__c,b.StudentStatus__c=d.StudentStatus__c,b.Student__c=d.Student__c,b.SystemModstamp=d.SystemModstamp,b.TempID__c=d.TempID__c,b.VehicleCount__c=d.VehicleCount__c,b.WalkingDegreeType__c=d.WalkingDegreeType__c,b.WalkinginCeremony__c=d.WalkinginCeremony__c,b.Walking_Degree__c=d.Walking_Degree__c,b.WhyWGU__c=d.WhyWGU__c,b.Created_YearMonth=d.Created_YearMonth WHEN NOT MATCHED THEN INSERT(AccommodationsFor__c,AccommodationsNeeded__c,AccommodationsType__c,AdditionalComments__c,AdditionalDegreeTimeframe__c,AdditionalDegree__c,AddressCanadaState__c,AddressCity__c,AddressCountry__c,AddressCounty__c,AddressLine1__c,AddressLine2__c,AddressNonUSState__c,AddressState__c,AddressUSState__c,AddressZip__c,CeremonyContactAddress__c,CeremonyContactEmail__c,CeremonyContactPhone__c,CeremonyLocale__c,CeremonyPeriod__c,CeremonyStatus__c,CeremonyYear__c,ConnectionReceivedId,ConnectionSentId,CreatedById,CreatedDate,DegreeCollege__c,DegreeProgramCode__c,DegreeProgramName__c,DegreeType__c,EligibilityStatus__c,Exemptions__c,FirstWGUDegree__c,FormStatus__c,GraduationDate__c,GraduationProgramCode__c,GraduationRecord__c,Grad_Date_Student_Process,GuestCount__c,Id,IsDeleted,LastActivityDate,LastModifiedById,LastModifiedDate,LastReferencedDate,LastViewedDate,LocationAppeals__c,MailingCity__c,MailingCountry__c,MailingStateProvince__c,MailingStreet__c,MailingZipPostalCode__c,MediaPermission__c,MilitaryCord__c,MilitaryServices__c,Name,OwnerId,PhotographyPermission__c,ProgramBookManualAdd__c,ProgramBookOptOut__c,Pronunciation__c,ProvidedStudentID__c,RecordTypeId,RemainingCUs__c,ReservedSeatingName__c,RSVPAcceptTexts__c,RSVPAddress__c,RSVPAlumniClbratnChildCnt,RSVPAlumniClbratnChildAtnding,RSVPAlumniCelebratnGuestCount,RSVPAttendingAlumniClbratn,RSVPCelebrationDegree__c,RSVPCollege__c,RSVPContinuingGrad__c,RSVPDateFirstDegree__c,RSVPDegree__c,RSVPGradAccommodations__c,RSVPGradASL__c,RSVPGradOtherDetails__c,RSVPGradOther__c,RSVPGradServiceAnimal__c,RSVPGradWheelchair__c,RSVPGuestAccommodations__c,RSVPGuestASL__c,RSVPGuestorGradAcomodation,RSVPGuestOtherDetails__c,RSVPGuestOther__c,RSVPGuestServiceAnimal__c,RSVPGuestWheelchair__c,RSVPMentorName__c,RSVPName__c,RSVPOtherFamilyWalkingName,RSVPOtherFamilyWalking__c,RSVPPhotographyDisclaimer__c,RSVPReasonNotAttending__c,RSVPReporters__c,RSVPShareStory__c,RSVPStudentStory__c,RSVPWhatWasFirstDegree__c,RSVP_First_Name__c,RSVP_Last_Name__c,StudentId__c,StudentMentorEmail__c,StudentMentor__c,StudentStatus__c,Student__c,SystemModstamp,TempID__c,VehicleCount__c,WalkingDegreeType__c,WalkinginCeremony__c,Walking_Degree__c,WhyWGU__c,Created_YearMonth) values (d.AccommodationsFor__c,d.AccommodationsNeeded__c,d.AccommodationsType__c,d.AdditionalComments__c,d.AdditionalDegreeTimeframe__c,d.AdditionalDegree__c,d.AddressCanadaState__c,d.AddressCity__c,d.AddressCountry__c,d.AddressCounty__c,d.AddressLine1__c,d.AddressLine2__c,d.AddressNonUSState__c,d.AddressState__c,d.AddressUSState__c,d.AddressZip__c,d.CeremonyContactAddress__c,d.CeremonyContactEmail__c,d.CeremonyContactPhone__c,d.CeremonyLocale__c,d.CeremonyPeriod__c,d.CeremonyStatus__c,d.CeremonyYear__c,d.ConnectionReceivedId,d.ConnectionSentId,d.CreatedById,d.CreatedDate,d.DegreeCollege__c,d.DegreeProgramCode__c,d.DegreeProgramName__c,d.DegreeType__c,d.EligibilityStatus__c,d.Exemptions__c,d.FirstWGUDegree__c,d.FormStatus__c,d.GraduationDate__c,d.GraduationProgramCode__c,d.GraduationRecord__c,d.Grad_Date_Student_Process ,d.GuestCount__c,d.Id,d.IsDeleted,d.LastActivityDate,d.LastModifiedById,d.LastModifiedDate,d.LastReferencedDate,d.LastViewedDate,d.LocationAppeals__c,d.MailingCity__c,d.MailingCountry__c,d.MailingStateProvince__c,d.MailingStreet__c,d.MailingZipPostalCode__c,d.MediaPermission__c,d.MilitaryCord__c,d.MilitaryServices__c,d.Name,d.OwnerId,d.PhotographyPermission__c,d.ProgramBookManualAdd__c,d.ProgramBookOptOut__c,d.Pronunciation__c,d.ProvidedStudentID__c,d.RecordTypeId,d.RemainingCUs__c,d.ReservedSeatingName__c,d.RSVPAcceptTexts__c,d.RSVPAddress__c,d.RSVPAlumniClbratnChildCnt,d.RSVPAlumniClbratnChildAtnding,d.RSVPAlumniCelebratnGuestCount,d.RSVPAttendingAlumniClbratn,d.RSVPCelebrationDegree__c,d.RSVPCollege__c,d.RSVPContinuingGrad__c,d.RSVPDateFirstDegree__c,d.RSVPDegree__c,d.RSVPGradAccommodations__c,d.RSVPGradASL__c,d.RSVPGradOtherDetails__c,d.RSVPGradOther__c,d.RSVPGradServiceAnimal__c,d.RSVPGradWheelchair__c,d.RSVPGuestAccommodations__c,d.RSVPGuestASL__c,d.RSVPGuestorGradAcomodation,d.RSVPGuestOtherDetails__c,d.RSVPGuestOther__c,d.RSVPGuestServiceAnimal__c,d.RSVPGuestWheelchair__c,d.RSVPMentorName__c,d.RSVPName__c,d.RSVPOtherFamilyWalkingName,d.RSVPOtherFamilyWalking__c,d.RSVPPhotographyDisclaimer__c,d.RSVPReasonNotAttending__c,d.RSVPReporters__c,d.RSVPShareStory__c,d.RSVPStudentStory__c,d.RSVPWhatWasFirstDegree__c,d.RSVP_First_Name__c,d.RSVP_Last_Name__c,d.StudentId__c,d.StudentMentorEmail__c,d.StudentMentor__c,d.StudentStatus__c,d.Student__c,d.SystemModstamp,d.TempID__c,d.VehicleCount__c,d.WalkingDegreeType__c,d.WalkinginCeremony__c,d.Walking_Degree__c,d.WhyWGU__c,d.Created_YearMonth)""" 
  val updateStmt = con.prepareStatement(merge_Oracle)
  updateStmt.executeUpdate(merge_Oracle)
  println("Data merged to target table for I value = "+i)
  j = j + incremental_value 
  merge_Oracle="" 
}
try
{
  if(con != null)
    con.close();
    println("Connection Closed")
}
catch
{
  case e: Exception => e.printStackTrace
}

// COMMAND ----------

// DBTITLE 1,Update the log file with latest modified timestamp
import org.apache.spark.sql.types._
var bi_commencement = sqlContext.sql(s"""select 'BI_C_COMMENCEMENT__C' as table_name, cast(max(LASTMODIFIEDDATE) as timestamp)  as LASTMODIFIEDDATE,from_utc_timestamp(current_timestamp, 'MST7MDT') as run_time from wgubisalesforce.BI_C_COMMENCEMENT
 """)  
bi_commencement.show()
bi_commencement.write.mode("Append").jdbc(url, "wgubisalesforce.DB_SALESFORCE_LOG", propProcessing)

// COMMAND ----------

bi_commencement = sqlContext.sql(s"""select 'BI_C_COMMENCEMENT__C' as table_name, cast(max(LASTMODIFIEDDATE) as timestamp)  as max_date_check,from_utc_timestamp(current_timestamp, 'MST7MDT') as run_time,count(*) as merge_count,count(*) as source_success_rows from wgubisalesforce.stg_db_commencement """)  
bi_commencement.show()
bi_commencement.write.mode("Append").jdbc(url, "wgubisalesforce.BI_WF_CTL_LOG", propProcessing)

// COMMAND ----------

dbutils.notebook.exit("Success")

// COMMAND ----------

// DBTITLE 1,QA
// MAGIC %sql 
// MAGIC --select 'DEV' AS Instance,* from wgubidev.bi_c_commencement where id in ('--a4S0c000000oOtZEAU' ,'a4S0c000000oOteEAE') --,'a4S0c000000oOtjEAE','a4S0c000000oOtoEAE','a4S0c000000oOttEAE','a4S0c000000oOtyEAE','a4S0c000000oOu3EAE','a4S0c000000oOu8EAE','a4S0c000000oOuDEAU','a4S0c000000oOuIEAU')
// MAGIC --union all
// MAGIC --select 'PROD' AS Instance,* from tempDF1 where id in ('--a4S0c000000oOtZEAU','a4S0c000000oOteEAE') --,'a4S0c000000oOtjEAE','a4S0c000000oOtoEAE','a4S0c000000oOttEAE','a4S0c000000oOtyEAE','a4S0c000000oOu3EAE','a4S0c000000oOu8EAE','a4S0c000000oOuDEAU','a4S0c000000oOuIEAU')
