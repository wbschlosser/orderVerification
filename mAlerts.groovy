import groovy.sql.Sql
import java.text.SimpleDateFormat

/*
 * File: mAlertsUpdate.groovy
 * Author: Wes Schlosser
 * Last Modified on: 8/23/13
 * Missing Orders Alert query
 * take all the orders in ATG processed in that status for a date
 * cross check with OMS to verify they're in OMS db
 * filter the results further, those NOT in exception and NOT in OMS are a big deal
 * automate email of results to relevant groups, find example on starTeam
 * incorporate user parameters java -jar missingalerts.jar date yes/no (for recursive) emailAddress
 * no arg[0] defaults to the default run (3 days prior only)
 * yes = from input day to 2 days prior to the day script is run
 * email address=input a desired email list for results to be sent to
 * 
 */
class MissingAlerts {

	static main(args) {
		//user influenced parameters
		def startDt, endDt = new Date() - 2
		def search = "no"
		def cList
		def atgStr
		//keeping track of order counts
		def atg = 0, oms = 0, diff = 0, exep = 0
		//keeping track of order numbers, formatting and emailing
		def omsAry = []
		def exepAry = []
		def results = "\norders not in OMS:\n"
		def sendEmail = true
		
		try{
			startDt = args[0]//date entered as yyyyMMdd
		}
		catch (Exception e){
			startDt = new Date() - 3//default day
			startDt = startDt.format("yyyyMMdd")
		}//end start date verification
		try{
			search = args[1]
		}
		catch (Exception e){
			//defaults to 'no' declared above
		}//end date range verification
		try{
			cList = args[2]
		}
		catch (Exception e){
			//custom email list is default
			cList = ''//default email list. Comma separated, no spaces between emails
		}//end email list verification
		
		if(search.toLowerCase() == "yes"){//no input or input less than 3 days (48 hour 'clear' window)
			endDt = endDt.format("yyyyMMdd")
			atgStr = "select ord.order_id " +
					 "FROM ATGPRDCORE.DCSPP_ORDER ord " +
					 "where ord.STATE in ('PROCESSING','NO_PENDING_ACTION') " +
					 "and ORDER_ID > '650000000' " +
					 "and to_char(SUBMITTED_DATE,'yyyymmdd') >= '$startDt' " +
					 "and to_char(SUBMITTED_DATE,'yyyymmdd') < '$endDt' " +
					 "minus select ko.order_id " +
					 "from atgprdcore.kls_order ko " +
					 "where order_type = 1"
		}
		else{//defaults to no
			//ATG query, default
			atgStr = "select ord.order_id " +
					 "FROM ATGPRDCORE.DCSPP_ORDER ord " +
					 "where ord.STATE in ('PROCESSING','NO_PENDING_ACTION') " +
					 "and ORDER_ID > '650000000' " +
					 "and to_char(SUBMITTED_DATE,'yyyymmdd') = '$startDt' " +
					 "minus select ko.order_id " +
					 "from atgprdcore.kls_order ko " +
					 "where order_type = 1"
		}
		
		//define and connect to ecom database
		def url = "server URL"
		def user = "userName"
		def password = "password"
		Sql dbEcom = Sql.newInstance(url, user, password,"oracle.jdbc.OracleDriver")
		
		//define and connect to OMS database
		url = "server URL"
		user = "userName"
		password = "password"
		Sql dbOms = Sql.newInstance(url, user, password,"oracle.jdbc.OracleDriver")
		
		println "running..."
		
		//queries from the ATG db and checks OMS for the order
		dbEcom.eachRow(atgStr) {orderID->
			 atg++
			 def boolFound = false
			 def omsStr ="select order_no " +
				 		 "FROM sterling.yfs_order_header oh where oh.order_no = '" + orderID[0] +
						 "' and oh.document_type = '0001'"
				 
			 dbOms.eachRow(omsStr){orderNo->
				 boolFound = true
				 oms++
			 }
			 if (boolFound == false){//order not found in OMS, checks for order in exception
				 def exceptStr = "select distinct extract(xmltype(message),'/Order/@OrderNo').getStringVal() " +
				 				 "from sterling.yfs_reprocess_error " +
								 "where STATE = 'Initial' " +
								 "and errortxnid > '$startDt' " +
								 "and extract(xmltype(message),'/Order/@OrderNo').getStringVal() = '" + orderID[0] + "'"
								 "minus select extract(xmltype(message),'/Order/@OrderNo').getStringVal() " +
								 "from sterling.yfs_reprocess_error " +
								 "where flow_name = 'CashEarnedActivationAsyncService' " +
								 "and errortxnid > '$startDt' " +
								 "and extract(xmltype(message),'/Order/@OrderNo').getStringVal() = '" + orderID[0] + "'"
								 
				try{//checks for issues with the XMLs
					dbOms.eachRow(exceptStr){order->
						boolFound = true
						exepAry[exep] = orderID[0]
						exep++
					}
				}
				catch (Exception e){
					println "XML issues with order number: " + orderID[0]
				}
		 
				if (boolFound == false){//order not found in OMS or exception
					 omsAry[diff] = orderID[0]
					 diff++
				}
			}
		}//end calculations
		
		for (def i = 0; i < omsAry.size;i++){
				results += omsAry[i] + "\n"
		}
		
		if(exep > 0){
			println "Orders in Exception:"
			print exepAry
		}
		
		results += "\nAmount from ATG: $atg" +
				   "\nAmount from OMS: $oms" +
				   "\nAmount in exception status: $exep" +
				   "\nAmount missing in OMS: $diff\n"
		print results
		
		//format date for the email
		startDt = startDt.substring(4)
		def month = startDt.substring(0,2)
		def date ="/"
		date += startDt.substring(2,4)
		startDt = month + date
		
		if (sendEmail == true){
			 def ant = new AntBuilder()
			 ant.mail(mailhost:'', mailport:'25', subject:"Missing Orders Alert from " + startDt, tolist:cList){
			   from(address:'')
			   replyto(address:'')
			   message(results)
			 }
		}
	}//end main
}
