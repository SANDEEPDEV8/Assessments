#include "StdAfx.h"

#include <iostream.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <ctype.h>

#include "RejectCodes.hpp" // 09/07/12 Igor

#include "ClaimsEngine.h"
#include "opusdef.h"
#include "miscutil.hpp"
#include "proc_clm.hpp"
#include "dbmsutil.hpp"
#include "grputil.hpp"
#include "analyze.hpp"
#include "Group.h"
#include "odbc/OdbcException.h"
#include "odbc/OdbcStatement.h"
#include "odbc/OdbcConnection.h"
#include "odbc/OdbcManager.h"

extern Claimant_Info  	Claimant_blk;
extern Group_Info		Group_blk;
extern Dispenser_Info	Dispenser_blk;

extern OdbcManager gOdbcManager;

extern SystemSettings System_Settings; // 05/31/12 Igor TP#19674
static int Trace_SQL = FALSE;

//extern char InternalSocket[15]; // 2/1/08 BSS
size_t to_upper(char* str);
const int	nMaxRows = 20;

// Purpose: Finds the Plan based on processor control number, and group number 
// Returns: Unique ID of Plan if found, 0 if not found or SQL error 
// 04/18/12 Igor TP#17644 - consolidated Get_Plan, New_Get_Group_ID and Get_Group_ID2 into Get_Group 
long Get_Group(char* Group_Number, long Group_ID, Group_Info &blk, BOOL isReversal, BOOL internalSocket) // 09/17/14 Igor TP#26614 - added internalSocket parameter
{
	Reporter reporter(MonitorFile, "Get_Group");

	char szStmt[1024];
	int UniqueID = 0;

	if (!Group_ID)
	{
		if (!Group_Number)
		{
			reporter.error("Invalid parameters");
			return SQL_PROCESSING_ERROR;
		}
		else if (!Group_Number[0]) // 08/25/14 Igor TP#26249
		{
			return RETURN_FAILURE;
		}
	}

	blk.Current_Patient_Copay = 99; // 1/21/03 BSS flag so that we know not to use this to override Patient_Copay

	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		// 08/11/14 Igor - replaced SQL with stored procedure to get values from CLAIMS.M_Plan
		strcpy_s(szStmt, sizeof(szStmt), "{CALL CLAIMS.usp_CE_GetPlan"
			"(@Group_Number=?"
			",@Group_ID=?)}");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (Group_Number != 0)
			pStmt->SetCharPtr(Group_Number);
		else
			pStmt->SetNull();

		if (Group_ID != 0)
			pStmt->SetInt(Group_ID);
		else
			pStmt->SetNull();

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (pStmt->First())
		{
			char GroupName[51] = { 0 };

			blk.Unique_ID = UniqueID = pStmt->GetLong("Unique_ID");
			pStmt->GetString("Name", GroupName, sizeof(GroupName), "N/A");
			pStmt->GetString("Group_ID", blk.Group_ID, sizeof(blk.Group_ID));
			pStmt->GetChar("Prescriber_Control", &blk.Prescriber_Control, '0');
			pStmt->GetChar("Provider_Control", &blk.Provider_Control, '0');
			pStmt->GetChar("Service_Control", &blk.Service_Control_Other_Group_Checking, '0');
			blk.Mail_Brand_AWP_Percentage = pStmt->GetNumeric("Mail_Brand_AWP_Percentage");
			blk.Mail_Brand_Fee = pStmt->GetNumeric("Mail_Brand_Fee");
			blk.Mail_Single_Generic_Fee = pStmt->GetNumeric("Mail_Single_Generic_Fee");
			blk.Mail_Multi_Generic_Fee = pStmt->GetNumeric("Mail_Multi_Generic_Fee");
			blk.Mail_Single_Generic_AWP_Percentage = pStmt->GetNumeric("Mail_Single_Generic_AWP_Percentage");
			blk.Mail_Multi_Generic_AWP_Percentage = pStmt->GetNumeric("Mail_Multi_Generic_AWP_Percentage");
			blk.Ind_Brand_AWP_Percentage = pStmt->GetNumeric("Ind_Brand_AWP_Percentage");
			blk.Ind_Brand_Fee = pStmt->GetNumeric("Ind_Brand_Fee");
			blk.Ind_Single_Generic_Fee = pStmt->GetNumeric("Ind_Single_Generic_Fee");
			blk.Ind_Multi_Generic_Fee = pStmt->GetNumeric("Ind_Multi_Generic_Fee");
			blk.Ind_Single_Generic_AWP_Percentage = pStmt->GetNumeric("Ind_Single_Generic_AWP_Percentage");
			blk.Ind_Multi_Generic_AWP_Percentage = pStmt->GetNumeric("Ind_Multi_Generic_AWP_Percentage");
			blk.Chn_Brand_AWP_Percentage = pStmt->GetNumeric("Chn_Brand_AWP_Percentage");
			blk.Chn_Brand_Fee = pStmt->GetNumeric("Chn_Brand_Fee");
			blk.Chn_Single_Generic_Fee = pStmt->GetNumeric("Chn_Single_Generic_Fee");
			blk.Chn_Multi_Generic_Fee = pStmt->GetNumeric("Chn_Multi_Generic_Fee");
			blk.Chn_Single_Generic_AWP_Percentage = pStmt->GetNumeric("Chn_Single_Generic_AWP_Percentage");
			blk.Chn_Multi_Generic_AWP_Percentage = pStmt->GetNumeric("Chn_Multi_Generic_AWP_Percentage");
			blk.Brand_Copay_Percentage = pStmt->GetNumeric("Brand_Copay_Percentage");
			blk.Single_Generic_Copay_Percentage = pStmt->GetNumeric("Single_Generic_Copay_Percentage");
			blk.Multi_Generic_Copay_Percentage = pStmt->GetNumeric("Multi_Generic_Copay_Percentage");
			blk.Single_Generic_Copay_Flat = pStmt->GetNumeric("Single_Generic_Copay_Flat");
			blk.Multi_Generic_Copay_Flat = pStmt->GetNumeric("Multi_Generic_Copay_Flat");
			blk.Brand_Copay_Flat = pStmt->GetNumeric("Brand_Copay_Flat");
			blk.Maximum_Refills = pStmt->GetNumeric("Maximum_Refills");
			blk.Maximum_Unit_Qty = pStmt->GetNumeric("Maximum_Unit_Qty");
			blk.Maximum_Days_Supply = pStmt->GetInt("Maximum_Days_Supply"); // 02/02/12 Igor TP#16709
			blk.Mail_Maximum_Days_Supply = pStmt->GetInt("Mail_Maximum_Days_Supply"); // 02/02/12 Igor TP#16709
			blk.Minimum_Days_Supply = pStmt->GetInt("Minimum_Days_Supply"); // 02/02/12 Igor TP#16709
			blk.Mail_Minimum_Days_Supply = pStmt->GetInt("Mail_Minimum_Days_Supply"); // 02/02/12 Igor TP#16709
			blk.Rule_ID = pStmt->GetLong("Rule_ID");
			blk.Additional_Pricing = pStmt->GetLong("Additional_Pricing");
			blk.Patient_Copay = pStmt->GetInt("Patient_CoPay");
			pStmt->GetDatePackedString("Expiration_Date", blk.ExpirationDate, sizeof(blk.ExpirationDate), "29991231");
			pStmt->GetDatePackedString("Terminate_Date", blk.TerminationDate, sizeof(blk.TerminationDate), "29991231");
			blk.SalesTax = pStmt->GetNumeric("Sales_Tax");
			blk.TwoTierPricing = pStmt->GetInt("Two_Tier_Pricing");;
			blk.Brand_TwoTier_Percentage = pStmt->GetNumeric("Brand_Two_Tier_Percent");
			blk.Single_Generic_Two_Tier_Percent = pStmt->GetNumeric("Single_Generic_Two_Tier_Percent");
			blk.Multi_Generic_Two_Tier_Percent = pStmt->GetNumeric("Multi_Generic_Two_Tier_Percent");
			blk.AdminFee = pStmt->GetNumeric("Admin_Fee");
			pStmt->GetChar("DESI_Restriction", &blk.DESI_Restriction, '0');
			blk.Days_Since_Filled = pStmt->GetInt("Days_Since_Filled");
			blk.Days_Since_Written = pStmt->GetInt("Days_Since_Written");
			pStmt->GetChar("Federal_Legend_Only", &blk.Federal_Legend_Only, '0');
			pStmt->GetString("TPA_Restrictions", blk.TPA_Restriction, sizeof(blk.TPA_Restriction), "0000000000000000000000000000000000000000000");
			pStmt->GetChar("Coverage_Code", &blk.Coverage_Code, 'I');// default to individual coverage
			pStmt->GetChar("Suspension", &blk.Suspension, 'N');// default to plan not suspended
			blk.Maximum_Metric_Qty = pStmt->GetNumeric("Maximum_Metric_Qty", 999.00); // default 999.00 to plan not suspended
			blk.Mail_Maximum_Metric_Qty = pStmt->GetNumeric("Mail_Maximum_Metric_Qty", blk.Mail_Maximum_Metric_Qty);
			blk.Processing_Charge = pStmt->GetNumeric("Processing_Charge");
			blk.Plan_Single_Generic_Fee = pStmt->GetNumeric("Plan_Single_Generic_Fee");
			blk.Plan_Multi_Generic_Fee = pStmt->GetNumeric("Plan_Multi_Generic_Fee");
			blk.Plan_Brand_Fee = pStmt->GetNumeric("Plan_Brand_Fee");
			pStmt->GetChar("MMIS_Reswitch_Flag", &blk.MMIS_Reswitch_Flag, 0);
			pStmt->GetChar("ResponseType", &blk.ResponseType, 'P'); //if no response type is defined then the default is PAID
			pStmt->GetString("LowMemberNumber", blk.LowMemberNumber, sizeof(blk.LowMemberNumber), 0);
			pStmt->GetString("HighMemberNumber", blk.HighMemberNumber, sizeof(blk.HighMemberNumber), 0);
			blk.MemberInterval = pStmt->GetInt("MemberInterval");
			blk.MemberNumberBaseOffset = pStmt->GetLong("MemberNumberBaseOffset");
			pStmt->GetString("Processor_Control", blk.Processor_Control, sizeof(blk.Processor_Control), 0);
			pStmt->GetChar("PayUCOnly", &blk.PayUCOnly, 'N');
			pStmt->GetChar("NoDUR", &blk.NoDUR, 'N');
			pStmt->GetChar("Eligibility_Override", &blk.Eligibility_Override);
			pStmt->GetChar("ZeroUandCisOK", &blk.ZeroUandCisOK);
			pStmt->GetChar("DawEdits", &blk.DawEdits, 'N');
			blk.MaxDispensings = pStmt->GetNumeric("MaxDispensings");
			blk.NumberOfDrugs = pStmt->GetInt("NumDrugs");
			blk.MemberNumberUniqueLength = pStmt->GetInt("MemberNumberUniqueLength");
			blk.AutoTerminateDays = pStmt->GetInt("AutoTerminateDays");
			pStmt->GetString("RxDenialOverridesAllowed", blk.RxDenialOverridesAllowed, sizeof(blk.RxDenialOverridesAllowed), 0);
			pStmt->GetChar("InteractionSeverity", &blk.InteractionSeverity, '3');
			blk.DuplicateTherapyThreashold = pStmt->GetInt("DuplicateTherapyThreashold", 100);
			blk.DiseaseThreashold = pStmt->GetInt("DiseaseThreashold");
			blk.PatientDataRequired = pStmt->GetInt("PatientDataRequired");
			pStmt->GetChar("Reswitch", &blk.Reswitch, 'N');
			pStmt->GetString("ReswitchBin", blk.ReswitchBin, sizeof(blk.ReswitchBin), 0);
			pStmt->GetString("ReswitchProcessor", blk.ReswitchProcessor, sizeof(blk.ReswitchProcessor), 0);
			pStmt->GetString("ReswitchGroup", blk.ReswitchGroup, sizeof(blk.ReswitchGroup), 0);
			pStmt->GetString("ReswitchCertificationID", blk.ReswitchCertificationID, sizeof(blk.ReswitchCertificationID), 0);
			blk.ReswitchFormat = pStmt->GetInt("ReswitchFormat");
			blk.Percent_For_Third_Party = pStmt->GetInt("Percent_For_Third_Party", -1);
			blk.Percent_For_Cash = pStmt->GetInt("Percent_For_Cash", -1);
			pStmt->GetString("OtherCoverageCodes", blk.OtherCoverageCodes, sizeof(blk.OtherCoverageCodes), 0);
			pStmt->GetChar("DebitRxPlan", &blk.DebitRxPlan, 0);
			if (blk.DebitRxPlan == 'N') // 01/28/20 Igor TP#42682 - stopped using 'N' type as it's really 0
				blk.DebitRxPlan = 0;
			blk.ReswitchTimeOut = pStmt->GetNumeric("ReswitchTimeOut"); // 05/23/18 Igor TP#38602 - converted ReswitchTimeOut to double
			blk.MaximumBenefitIndividual = pStmt->GetNumeric("Maximum_Benefit_Individual");
			blk.MaximumBenefitFamily = pStmt->GetNumeric("Maximum_Benefit_Family");
			pStmt->GetChar("IgnoreIndividMaxBenefitWhenFamilyExists", &blk.IgnoreIndividualBenefitWhenFamilyExists);
			pStmt->GetDatePackedString("Effective_Date", blk.Effective_Date, sizeof(blk.Effective_Date), "20000101");
			pStmt->GetString("ValidStatusCodes", blk.ValidStatusCodes, sizeof(blk.ValidStatusCodes));
			// 01/06/15 Troy TP#28029 - Separating this into cash and insured
			//blk.NextFillAllowedAfter = pStmt->GetInt("NextFillAllowedAfter", 100);// 100 percent of the previous filled days supply must be use before it can be refilled
			pStmt->GetString("ProgramPhoneNumber", blk.ProgramPhoneNumber, sizeof(blk.ProgramPhoneNumber), 0);
			//pStmt->GetString("PharmaProgramName", blk.PharmaProgramName, sizeof(blk.PharmaProgramName), 0); // 08/23/19 Igor TP#41546
			blk.RejectOnZeroBenefit = pStmt->GetInt("RejectOnZeroBenefit");
			blk.GovernmentCoverageRules = pStmt->GetInt("GovernmentCoverageRules");
			blk.DispenserTypeRule = pStmt->GetInt("DispenserTypeRule");
			blk.MedicareChecking = pStmt->GetInt("MedicareChecking");
			pStmt->GetChar("MedicareLevelOfChecking", &blk.MedicareLevelOfChecking, 'W');
			blk.EffectiveDateOffset = pStmt->GetInt("EffectiveDateOffset");
			blk.ManualAdminFee = pStmt->GetNumeric("Manual_Admin_Fee", -1.00);
			blk.ManualAdminFeePatient = pStmt->GetNumeric("Manual_Admin_Fee_Patient", -1.00);
			blk.ManualRemitFee = pStmt->GetNumeric("Manual_Remit_Fee", -1.00);
			blk.CheckSumRule = pStmt->GetInt("CheckSumRule");
			blk.CheckSumModValue = pStmt->GetInt("CheckSumModValue");
			pStmt->GetString("MedicareRspMsg", blk.MedicareRspMsg, sizeof(blk.MedicareRspMsg), 0);
			blk.WildCardNDC = pStmt->GetInt("WildCardNDC");
			blk.CPL_Rule = pStmt->GetInt("MetaphoneMatching");
			// 01/06/15 Troy TP#28029 - Separating this into cash and insured
			//blk.MailNextFillAllowedAfter = pStmt->GetInt("MailNextFillAllowedAfter", blk.NextFillAllowedAfter);
			pStmt->GetString("MailRxDenialOverridesAllowed", blk.MailRxDenialOverridesAllowed, sizeof(blk.MailRxDenialOverridesAllowed));
			blk.Mail_Maximum_Refills = pStmt->GetNumeric("MailMaximumRefills", blk.Maximum_Refills);
			blk.ReimbursementType = pStmt->GetInt("ReimbursementType");
			blk.MinimumAge = pStmt->GetInt("MinimumAge");
			pStmt->GetDatePackedString("FillByDateCash", blk.FillByDateCash, sizeof(blk.FillByDateCash), 0);
			pStmt->GetDatePackedString("FillByDateInsured", blk.FillByDateInsured, sizeof(blk.FillByDateInsured), 0);
			blk.Reimbursement_Method = pStmt->GetInt("ReimbursementMethod");
			blk.WacPercentage = pStmt->GetNumeric("WacPercentage");
			blk.WacClaimTypes = pStmt->GetInt("WacClaimTypes");
			pStmt->GetString("AuthorizedBin", blk.AuthorizedBin, sizeof(blk.AuthorizedBin), 0);
			pStmt->GetChar("FillsOrPills", &blk.FillsOrPills, 0);
			blk.NextFillAllowedAfterRule = pStmt->GetInt("NextFillAllowedAfterRule");
			blk.MetaPhoneLinkID = pStmt->GetInt("MetaPhoneLinkID");
			blk.Num_Products = pStmt->GetInt("Num_Products");
			pStmt->GetString("IgnoreRejectCodeList", blk.IgnoreRejectCodeList, sizeof(blk.IgnoreRejectCodeList), 0);
			pStmt->GetString("ValidPersonCodes", blk.ValidPersonCodes, sizeof(blk.ValidPersonCodes), 0);
			pStmt->GetString("MedicarePaidMessage", blk.MedicarePaidMessage, sizeof(blk.MedicarePaidMessage), 0);
			pStmt->GetString("MedicareRejectMessage", blk.MedicareRejectMessage, sizeof(blk.MedicareRejectMessage), 0);
			blk.MultiBenefitRule = pStmt->GetInt("MultiBenefitRule");
			blk.MailMultiBenefitRule = pStmt->GetInt("MailMultiBenefitRule");
			blk.ProgramHasBeenExtended = pStmt->GetInt("ProgramHasBeenExtended");
			blk.PatientPayThresholdForVoucherMsg = pStmt->GetNumeric("PatientPayThresholdForVoucherMsg");
			blk.RequireOriginalScript = pStmt->GetInt("RequireOriginalScript");
			blk.MultiBenefitDaysPerDisp = pStmt->GetInt("MultiBenefitDaysPerDisp");
			blk.MailMultiBenefitDaysPerDisp = pStmt->GetInt("MailMultiBenefitDaysPerDisp");
			pStmt->GetChar("MaxIndividBenefitExceededBehavior", &blk.MaxIndividBenefitExceededBehavior, 'R');// default is to Reject when max benefit is exceeded
			blk.Mail_Minimum_Metric_Qty = pStmt->GetNumeric("Mail_Minimum_Metric_Qty");
			blk.Minimum_Metric_Qty = pStmt->GetNumeric("Minimum_Metric_Qty");
			pStmt->GetString("ProgramExtensionMessage", blk.ProgramExtensionMessage, sizeof(blk.ProgramExtensionMessage), 0);
			pStmt->GetString("QtyToDaySupplyRatio", blk.QtyToDaySupplyRatio, sizeof(blk.QtyToDaySupplyRatio), 0);
			blk.DualCardPlanID = pStmt->GetLong("DualCardPlanID");
			blk.ForceReversalsInOrder = pStmt->GetInt("ForceReversalsInOrder");
			blk.MinPatPayAmtCash = pStmt->GetNumeric("MinPatPayAmtCash");
			blk.MinPatPayAmtInsured = pStmt->GetNumeric("MinPatPayAmtInsured");
			blk.MailMinPatPayAmtCash = pStmt->GetNumeric("MailMinPatPayAmtCash", blk.MinPatPayAmtCash >= 0 ? blk.MinPatPayAmtCash : 0);
			blk.MailMinPatPayAmtInsured = pStmt->GetNumeric("MailMinPatPayAmtInsured", blk.MinPatPayAmtInsured >= 0 ? blk.MinPatPayAmtInsured : 0);
			blk.Ind_Brand_WAC_Percentage = pStmt->GetNumeric("Ind_Brand_WAC_Percentage");
			blk.Ind_Single_Generic_WAC_Percentage = pStmt->GetNumeric("Ind_Single_Generic_WAC_Percentage");
			blk.Ind_Multi_Generic_WAC_Percentage = pStmt->GetNumeric("Ind_Multi_Generic_WAC_Percentage");
			blk.Mail_Brand_WAC_Percentage = pStmt->GetNumeric("Mail_Brand_WAC_Percentage");
			blk.Mail_Single_Generic_WAC_Percentage = pStmt->GetNumeric("Mail_Single_Generic_WAC_Percentage");
			blk.Mail_Multi_Generic_WAC_Percentage = pStmt->GetNumeric("Mail_Multi_Generic_WAC_Percentage");
			blk.Chn_Brand_WAC_Percentage = pStmt->GetNumeric("Chn_Brand_WAC_Percentage");
			blk.Chn_Single_Generic_WAC_Percentage = pStmt->GetNumeric("Chn_Single_Generic_WAC_Percentage");
			blk.Chn_Multi_Generic_WAC_Percentage = pStmt->GetNumeric("Chn_Multi_Generic_WAC_Percentage");
			blk.ForceClaimsInOrder = pStmt->GetInt("ForceClaimsInOrder");
			blk.PercentThresholdForCashPay = pStmt->GetNumeric("PercentThresholdForCashPay");
			blk.MaximumBenefitCash = pStmt->GetNumeric("MaximumBenefitCash");
			blk.MaximumBenefitInsured = pStmt->GetNumeric("MaximumBenefitInsured");
			blk.MailMaximumBenefitCash = pStmt->GetNumeric("MailMaximumBenefitCash", blk.MaximumBenefitCash);
			blk.MailMaximumBenefitInsured = pStmt->GetNumeric("MailMaximumBenefitInsured", blk.MaximumBenefitInsured);
			blk.Specialty_Brand_AWP_Percentage = pStmt->GetNumeric("Specialty_Brand_AWP_Percentage");
			blk.Specialty_Brand_Fee = pStmt->GetNumeric("Specialty_Brand_Fee");
			blk.Specialty_Single_Generic_AWP_Percentage = pStmt->GetNumeric("Specialty_Single_Generic_AWP_Percentage");
			blk.Specialty_Multi_Generic_AWP_Percentage = pStmt->GetNumeric("Specialty_Multi_Generic_AWP_Percentage");
			blk.Specialty_Single_Generic_Fee = pStmt->GetNumeric("Specialty_Single_Generic_Fee");
			blk.Specialty_Multi_Generic_Fee = pStmt->GetNumeric("Specialty_Multi_Generic_Fee");
			blk.Specialty_Maximum_Days_Supply = pStmt->GetInt("Specialty_Maximum_Days_Supply");
			blk.Specialty_Maximum_Metric_Qty = pStmt->GetNumeric("Specialty_Maximum_Metric_Qty");
			blk.SpecialtyMaximumBenefitCash = pStmt->GetNumeric("SpecialtyMaximumBenefitCash");
			blk.SpecialtyMaximumBenefitInsured = pStmt->GetNumeric("SpecialtyMaximumBenefitInsured");
			blk.SpecialtyMaximumRefills = pStmt->GetInt("SpecialtyMaximumRefills");
			blk.SpecialtyMinPatPayAmtCash = pStmt->GetNumeric("SpecialtyMinPatPayAmtCash");
			blk.SpecialtyMinPatPayAmtInsured = pStmt->GetNumeric("SpecialtyMinPatPayAmtInsured");
			blk.SpecialtyMultiBenefitRule = pStmt->GetInt("SpecialtyMultiBenefitRule");
			// 01/06/15 Troy TP#28029 - Separating this into cash and insured
			//blk.SpecialtyNextFillAllowedAfter = pStmt->GetInt("SpecialtyNextFillAllowedAfter");
			pStmt->GetString("SpecialtyRxDenialOverridesAllowed", blk.SpecialtyRxDenialOverridesAllowed, sizeof(blk.SpecialtyRxDenialOverridesAllowed));
			blk.SpecialtyMinimumDaysSupply = pStmt->GetInt("SpecialtyMinimumDaysSupply");
			blk.SpecialtyMinimumMetricQty = pStmt->GetNumeric("SpecialtyMinimumMetricQty");
			blk.Specialty_Brand_WAC_Percentage = pStmt->GetNumeric("Specialty_Brand_WAC_Percentage");
			blk.Specialty_Single_Generic_WAC_Percentage = pStmt->GetNumeric("Specialty_Single_Generic_WAC_Percentage");
			blk.Specialty_Multi_Generic_WAC_Percentage = pStmt->GetNumeric("Specialty_Multi_Generic_WAC_Percentage");
			blk.MaxBenefitIndividualRule = pStmt->GetInt("MaxBenefitIndividualRule");
			blk.EnforceMinPatPayAmtMultiBenefitRules = pStmt->GetInt("EnforceMinPatPayAmtMultiBenefitRules");
			blk.RequireICDCodes = pStmt->GetInt("RequireICDCodes");
			blk.ServiceBillingPlanRules = pStmt->GetInt("ServiceBillingPlanRules");
			blk.InsuranceVerificationRule = pStmt->GetInt("InsuranceVerificationRule");
			blk.InsuranceVerificationLookbackPeriod = pStmt->GetInt("InsuranceVerificationLookbackPeriod");
			blk.InsuranceVerificationUniquePARequired = pStmt->GetInt("InsuranceVerificationUniquePARequired"); // 8/29/10 BSS MSOE-63
			blk.CustomRejectRules = pStmt->GetInt("CustomRejectRules");
			blk.QtyMultipleOfDaysSupply = pStmt->GetInt("QtyMultipleOfDaysSupply");
			blk.PayCountOfMultiple = pStmt->GetNumeric("PayCountOfMultiple");
			blk.PreloadAutoTerminateDays = pStmt->GetInt("PreloadAutoTerminateDays");
			blk.NotAllCardholderIDsAreUnique = pStmt->GetInt("NotAllCardholderIDsAreUnique");
			blk.PatientPayThresholdMsg = pStmt->GetNumeric("PatientPayThresholdMsg");
			blk.Opus_Brand_Fee = pStmt->GetNumeric("Opus_Brand_Fee");
			blk.Opus_Single_Generic_Fee = pStmt->GetNumeric("Opus_Single_Generic_Fee");
			blk.Opus_Multi_Generic_Fee = pStmt->GetNumeric("Opus_Multi_Generic_Fee");
			blk.Sponsor_Brand_Fee = pStmt->GetNumeric("Sponsor_Brand_Fee");
			blk.Sponsor_Single_Generic_Fee = pStmt->GetNumeric("Sponsor_Single_Generic_Fee");
			blk.Sponsor_Multi_Generic_Fee = pStmt->GetNumeric("Sponsor_Multi_Generic_Fee");
			blk.Ind_Brand_Cost_Basis = pStmt->GetInt("Ind_Brand_Cost_Basis");
			blk.Ind_Single_Generic_Cost_Basis = pStmt->GetInt("Ind_Single_Generic_Cost_Basis");
			blk.Ind_Multi_Generic_Cost_Basis = pStmt->GetInt("Ind_Multi_Generic_Cost_Basis");
			blk.Chn_Brand_Cost_Basis = pStmt->GetInt("Chn_Brand_Cost_Basis");
			blk.Chn_Single_Generic_Cost_Basis = pStmt->GetInt("Chn_Single_Generic_Cost_Basis");
			blk.Chn_Multi_Generic_Cost_Basis = pStmt->GetInt("Chn_Multi_Generic_Cost_Basis");
			blk.Mail_Brand_Cost_Basis = pStmt->GetInt("Mail_Brand_Cost_Basis");
			blk.Mail_Single_Generic_Cost_Basis = pStmt->GetInt("Mail_Single_Generic_Cost_Basis");
			blk.Mail_Multi_Generic_Cost_Basis = pStmt->GetInt("Mail_Multi_Generic_Cost_Basis");
			blk.Specialty_Brand_Cost_Basis = pStmt->GetInt("Specialty_Brand_Cost_Basis");
			blk.Specialty_Single_Generic_Cost_Basis = pStmt->GetInt("Specialty_Single_Generic_Cost_Basis");
			blk.Specialty_Multi_Generic_Cost_Basis = pStmt->GetInt("Specialty_Multi_Generic_Cost_Basis");
			blk.Ind_Brand_Reimbursement_Formula = pStmt->GetInt("Ind_Brand_Reimbursement_Formula");
			blk.Ind_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Ind_Single_Generic_Reimbursement_Formula");
			blk.Ind_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Ind_Multi_Generic_Reimbursement_Formula");
			blk.Chn_Brand_Reimbursement_Formula = pStmt->GetInt("Chn_Brand_Reimbursement_Formula");
			blk.Chn_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Chn_Single_Generic_Reimbursement_Formula");
			blk.Chn_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Chn_Multi_Generic_Reimbursement_Formula");
			blk.Mail_Brand_Reimbursement_Formula = pStmt->GetInt("Mail_Brand_Reimbursement_Formula");
			blk.Mail_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Mail_Single_Generic_Reimbursement_Formula");
			blk.Mail_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Mail_Multi_Generic_Reimbursement_Formula");
			blk.Specialty_Brand_Reimbursement_Formula = pStmt->GetInt("Specialty_Brand_Reimbursement_Formula");
			blk.Specialty_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Specialty_Single_Generic_Reimbursement_Formula");
			blk.Specialty_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Specialty_Multi_Generic_Reimbursement_Formula");
			blk.OtherPayerBINTypeRules = pStmt->GetInt("OtherPayerBINTypeRules");
			blk.StepEditRules = pStmt->GetInt("StepEditRules");
			blk.PrimaryRejectCodesAction = pStmt->GetInt("PrimaryRejectCodesAction");
			blk.FillByDateCashDispensing = pStmt->GetNumeric("FillByDateCashDispensing");
			blk.FillByDateInsuredDispensing = pStmt->GetNumeric("FillByDateInsuredDispensing");
			blk.FillAfterDateCashDispensing = pStmt->GetNumeric("FillAfterDateCashDispensing");
			blk.FillAfterDateInsuredDispensing = pStmt->GetNumeric("FillAfterDateInsuredDispensing");
			pStmt->GetDatePackedString("FillAfterDateCash", blk.FillAfterDateCash, sizeof(blk.FillAfterDateCash), 0);
			pStmt->GetDatePackedString("FillAfterDateInsured", blk.FillAfterDateInsured, sizeof(blk.FillAfterDateInsured), 0);
			blk.PortalAdminFeePatient = pStmt->GetNumeric("Portal_Admin_Fee_Patient", -1);
			blk.PortalAdminFeePharmacy = pStmt->GetNumeric("Portal_Admin_Fee_Pharmacy", -1);
			blk.PortalRemitFee = pStmt->GetNumeric("Portal_Remit_Fee", -1);
			blk.AllowCompoundDrugs = pStmt->GetInt("AllowCompoundDrugs"); // 03/18/13 Igor TP#23177
			// 01/06/15 Troy TP#28029 - Separating this into cash and insured
			//blk.MinimumDaysBetweenPaidClaims = pStmt->GetInt("MinimumDaysBetweenPaidClaims"); // 03/29/13 Igor TP#23548
			blk.MedicareVerificationRule = pStmt->GetInt("MedicareVerificationRule"); // 06/20/13 Igor TP#23705 
			blk.MedicareVerificationLookbackPeriod = pStmt->GetInt("MedicareVerificationLookbackPeriod"); // 06/20/13 Igor TP#23705 
			blk.MedicareVerificationUniquePARequired = pStmt->GetInt("MedicareVerificationUniquePARequired"); // 06/20/13 Igor TP#23705 
			blk.MedicareOverrideRule = pStmt->GetInt("MedicareOverrideRule");			// 06/26/13 Igor TP#23750
			blk.MedicareOverridePeriod = pStmt->GetInt("MedicareOverridePeriod");		// 06/26/13 Igor TP#23750
			blk.MedicareOverrideScope = pStmt->GetInt("MedicareOverrideScope");			// 06/26/13 Igor TP#23750
			blk.MedicareOverrideDateType = pStmt->GetInt("MedicareOverrideDateType");	// 06/26/13 Igor TP#23750
			blk.ValidatePreloadDOB = pStmt->GetInt("ValidatePreloadDOB");									// 08/22/13 Igor TP#24069
			blk.GovtCoverageAttestationRule = pStmt->GetInt("GovtCoverageAttestationRule");					// 08/22/13 Igor TP#24069
			blk.GovtCoverageAttestationValidateDays = pStmt->GetInt("GovtCoverageAttestationValidateDays"); // 08/22/13 Igor TP#24069
			blk.GovtCoverageAttestationValidateAge = pStmt->GetInt("GovtCoverageAttestationValidateAge");	// 08/22/13 Igor TP#24069
			blk.OpusFeeFloor = pStmt->GetNumeric("OpusFeeFloor", -1);			// 09/11/13 Igor TP#24151
			blk.SponsorFeeFloor = pStmt->GetNumeric("SponsorFeeFloor", -1);		// 09/11/13 Igor TP#24151
			blk.OpusFeeCeiling = pStmt->GetNumeric("OpusFeeCeiling");			// 09/11/13 Igor TP#24151
			blk.SponsorFeeCeiling = pStmt->GetNumeric("SponsorFeeCeiling");		// 09/11/13 Igor TP#24151
			blk.PrescriberOverrideRules = pStmt->GetInt("PrescriberOverrideRules");		// 10/08/13 Troy TP#24323
			blk.UseHistoricalData = pStmt->GetInt("UseHistoricalData");			// 10/18/13 Igor TP#24355
			pStmt->GetString("CalculateBenefitSP", blk.VariableBenefitCustomSP, sizeof(blk.VariableBenefitCustomSP), 0); // 12/07/14 Igor TP#27755 - ability to call a custom SP for determining Max Benefit and Min Patient Pay Amount
			blk.MinimumAgeType = pStmt->GetInt("MinimumAgeType"); // 06/18/21 Igor TP#47568
			
			// 09/06/22 Igor - updated log messages
			char groupInfo[500] = { 0 }, sub[100] = { 0 }; // 11/10/22 Igor - increased sub size from 50 to 100
			sprintf_s(groupInfo, sizeof(groupInfo), "Group <%s> <%s>, ID <%ld>", blk.Group_ID, StripExtraSpaces(GroupName), UniqueID);
			if (blk.DebitRxPlan)
			{
				sprintf_s(sub, sizeof(sub), ", DebitRx <%c>", blk.DebitRxPlan);
				strcat_s(groupInfo, sizeof(groupInfo), sub);
			}

			sub[0] = 0;
			if (pStmt->IsNull("Eligibility_Check"))
			{
				blk.EligibilityCheck = 'A';   // default to member number checking
				strcpy_s(sub, sizeof(sub), ", Eligibility_Check <A> (Check Member Number)");
				blk.PreLoadRequired = FALSE;
			}
			else
			{
				pStmt->GetChar("Eligibility_Check", &blk.EligibilityCheck);
				if (blk.EligibilityCheck == 'S' || blk.EligibilityCheck == 'P'
					|| blk.EligibilityCheck == 'K') // 04/05/21 Igor TP#46060
				{
					if (blk.EligibilityCheck == 'S')
					{
						blk.StaticMember = TRUE;
						strcpy_s(sub, sizeof(sub), ", Eligibility_Check <S> (Static Member Numbers)");
					}
					else if (blk.EligibilityCheck == 'K')
					{
						blk.KeepClaimant = TRUE;
						strcpy_s(sub, sizeof(sub), ", Eligibility_Check <K> (Keep Claimant Record)");
					}
					else // blk.Eligibility_Check == 'P'
					{
						blk.PreLoadRequired = TRUE;
						strcpy_s(sub, sizeof(sub), ", Eligibility_Check <P> (PreLoad Validation Required)");
					}
				}
				else
				{
					blk.PreLoadRequired = FALSE;
					sprintf_s(sub, sizeof(sub), ", Eligibility_Check <%c>", blk.EligibilityCheck);
				}
			}
			strcat_s(groupInfo, sizeof(groupInfo), sub);
			reporter.info(groupInfo);
			/*
			if (blk.DebitRxPlan)
				reporter.info("Group <%s> <%s>, ID <%ld>, DebitRx <%c>", blk.Group_ID, StripExtraSpaces(GroupName), UniqueID, blk.DebitRxPlan);
			else
				reporter.info("Group <%s> <%s>, ID <%ld>", blk.Group_ID, StripExtraSpaces(GroupName), UniqueID);

			char buf[30] = { 0 };
			if (pStmt->IsNull("Eligibility_Check"))
			{
				blk.EligibilityCheck = 'A';   // default to member number checking
				strcpy_s(buf, sizeof(buf), "Check Member Number");
				blk.PreLoadRequired = FALSE;
			}
			else
			{
				pStmt->GetChar("Eligibility_Check", &blk.EligibilityCheck);
				if (blk.EligibilityCheck == 'S' || blk.EligibilityCheck == 'P'
					|| blk.EligibilityCheck == 'K') // 04/05/21 Igor TP#46060
				{
					if (blk.EligibilityCheck == 'S')
					{
						blk.StaticMember = TRUE;
						strcpy_s(buf, sizeof(buf), "Static Member Numbers");
					}
					else if (blk.EligibilityCheck == 'K')
					{
						blk.KeepClaimant = TRUE;
						strcpy_s(buf, sizeof(buf), "Keep Claimant Record");
					}
					else // blk.Eligibility_Check == 'P'
					{
						blk.PreLoadRequired = TRUE;
						strcpy_s(buf, sizeof(buf), "PreLoad Validation Required");
					}
				}
				else
					blk.PreLoadRequired = FALSE;
			}

			// 04/05/21 Igor TP#46060
			reporter.info("Group Eligibility_Check <%c> (%s)", blk.EligibilityCheck, buf);
			*/

			if (blk.StaticMember || blk.KeepClaimant || blk.PreLoadRequired)
				blk.EligibilityCheck = 'O';

			// 1/30/06 BSS back door for a non expired program where we want to stop all new users from filling
			if (blk.MaxDispensings < 0)
			{
				blk.SetMaxDispensingsToZero = 'Y';
				blk.MaxDispensings *= -1;
			}
			// 4/5/07 BSS need to treat internally patient check programs as debit rx programs
			switch (blk.Reimbursement_Method)
			{
			case 1:
				blk.DebitRxPlan = 'Y';
				break;
			case 3:
			{
				// if its type '3' and its debit and we are on the internal socket then good
				if ((blk.DebitRxPlan == 'Y') && internalSocket)
					reporter.info("ReimbursementMethod is type 3 - Debit Program will be processed as mail-in (no card funding)");
				else
					blk.Reimbursement_Method = 0;
				break;
			}
			}
			blk.MinPatPayAmtRule = pStmt->GetInt("MinPatPayAmtRule");			// 01/27/14 Igor TP#24815
			// 01/27/14 Igor TP#24815
			blk.MinPatPayCopayPercentageCash = pStmt->GetInt("MinPatPayCopayPercentageCash");
			blk.MinPatPayCopayPercentageInsured = pStmt->GetInt("MinPatPayCopayPercentageInsured");
			blk.MailMinPatPayCopayPercentageCash = pStmt->GetInt("MailMinPatPayCopayPercentageCash");
			blk.MailMinPatPayCopayPercentageInsured = pStmt->GetInt("MailMinPatPayCopayPercentageInsured");
			blk.SpecialtyMinPatPayCopayPercentageCash = pStmt->GetInt("SpecialtyMinPatPayCopayPercentageCash");
			blk.SpecialtyMinPatPayCopayPercentageInsured = pStmt->GetInt("SpecialtyMinPatPayCopayPercentageInsured");
			if (!pStmt->IsNull("EnrollmentDays")) // 10/14/20 Igor TP#44788 - added validation for NULL value
			{
				blk.ProcessEnrollment = TRUE;
				blk.EnrollmentDays = pStmt->GetInt("EnrollmentDays"); // 02/10/14 Igor TP#24905 
			}
			blk.EnrollmentLookBackDays = pStmt->GetInt("EnrollmentLookBackDays");//06/26/14 Igor TP#25658
			blk.ReEnrollmentLookBackDays = pStmt->GetInt("ReEnrollmentLookBackDays");	// 06/01/15 Igor TP#29267
			blk.EnrollmentTreashold = pStmt->GetInt("EnrollmentTreashold");	// 06/18/15 Igor
			blk.BenefitStartDateThreshold = pStmt->GetInt("BenefitStartDateThreshold"); // 02/19/14 Igor TP#24848
			blk.CostLimitPercent = pStmt->GetNumeric("CostLimitPercent");			// 07/14/14 Igor TP#25846 // 03/04/21 Igor TP#46665 - converted to double
			blk.CardActivationRule = pStmt->GetInt("CardActivationRule");		// 09/25/14 Igor TP#26713
			// 12/05/14 Igor TP#27757 - separated the VariableBenefitRule into VariableBenefitRuleCash and VariableBenefitRuleInsured
			blk.VariableBenefitRuleCash = pStmt->GetInt("VariableBenefitRuleCash");
			blk.VariableBenefitRuleInsured = pStmt->GetInt("VariableBenefitRuleInsured");
			blk.MaxDosagePerDay = pStmt->GetNumeric("MaxDosagePerDay");			// 12/29/14 Troy TP#27998
			blk.NextFillAllowedAfterCash = pStmt->GetInt("NextFillAllowedAfterCash");	// 01/06/15 Troy TP#28029
			blk.NextFillAllowedAfterInsured = pStmt->GetInt("NextFillAllowedAfterInsured");	// 01/06/15 Troy TP#28029
			blk.NextFillAllowedAfterInsNotCovered = pStmt->GetInt("NextFillAllowedAfterInsNotCovered"); // 06/30/20 Igor TP#44204
			blk.MailNextFillAllowedAfterCash = pStmt->GetInt("MailNextFillAllowedAfterCash", blk.NextFillAllowedAfterCash);		// 01/06/15 Troy TP#28029
			blk.MailNextFillAllowedAfterInsured = pStmt->GetInt("MailNextFillAllowedAfterInsured", blk.NextFillAllowedAfterInsured);		// 01/06/15 Troy TP#28029
			blk.MailNextFillAllowedAfterInsNotCovered = pStmt->GetInt("MailNextFillAllowedAfterInsNotCovered"); // 06/30/20 Igor TP#44204
			blk.SpecialtyNextFillAllowedAfterCash = pStmt->GetInt("SpecialtyNextFillAllowedAfterCash", blk.NextFillAllowedAfterCash);		// 01/06/15 Troy TP#28029
			blk.SpecialtyNextFillAllowedAfterInsured = pStmt->GetInt("SpecialtyNextFillAllowedAfterInsured", blk.NextFillAllowedAfterInsured);		// 01/06/15 Troy TP#28029
			blk.SpecialtyNextFillAllowedAfterInsNotCovered = pStmt->GetInt("SpecialtyNextFillAllowedAfterInsNotCovered"); // 06/30/20 Igor TP#44204
			blk.MinimumDaysBetweenPaidClaimsCash = pStmt->GetInt("MinimumDaysBetweenPaidClaimsCash");	// 01/06/15 Troy TP#28029
			blk.MinimumDaysBetweenPaidClaimsInsured = pStmt->GetInt("MinimumDaysBetweenPaidClaimsInsured");	// 01/06/15 Troy TP#28029
			pStmt->GetString("CustomCPLSP", blk.CustomCPLSP, sizeof(blk.CustomCPLSP)); // 01/08/15 Igor TP#28031
			blk.CardActivationLinkIdentifier = pStmt->GetLong("CardActivationLinkIdentifier"); // 02/27/15 Igor TP#28236
			blk.GovtCoverageCopayThreshold = pStmt->GetNumeric("GovtCoverageCopayThreshold"); // 08/05/15 Igor TP#29814
			// 10/27/15 Igor TP#30346
			blk.MaxBenefitInsNotCovered = pStmt->GetNumeric("MaxBenefitInsNotCovered");
			blk.MailMaxBenefitInsNotCovered = pStmt->GetNumeric("MailMaxBenefitInsNotCovered");
			blk.SpecialtyMaxBenefitInsNotCovered = pStmt->GetNumeric("SpecialtyMaxBenefitInsNotCovered");
			blk.MinPatPayAmtInsNotCovered = pStmt->GetNumeric("MinPatPayAmtInsNotCovered");
			blk.MailMinPatPayAmtInsNotCovered = pStmt->GetNumeric("MailMinPatPayAmtInsNotCovered");
			blk.SpecialtyMinPatPayAmtInsNotCovered = pStmt->GetNumeric("SpecialtyMinPatPayAmtInsNotCovered");
			blk.PercentForInsNotCovered = pStmt->GetInt("PercentForInsNotCovered", -1);
			// 12/02/15 Igor TP#30670
			char Date[PACKED_DATE_SIZE] = { 0 };
			pStmt->GetDatePackedString("AdjustEnrollTerminateDates", Date, sizeof(Date), 0);
			blk.AdjustEnrollAndTerminateDates = Date[0] != 0 && Today2Num() <= Date2Num(Date);
			pStmt->GetChar("MinimumDaysBetweenPaidClaimsCashOverrideType", &blk.MinimumDaysBetweenPaidClaimsCashOverrideType);	// 02/08/16 Igor TP#31210
			pStmt->GetChar("MinimumDaysBetweenPaidClaimsInsuredOverrideType", &blk.MinimumDaysBetweenPaidClaimsInsuredOverrideType); // 02/08/16 Igor TP#31210
			blk.AncillaryBusinessRules = pStmt->GetInt("AncillaryBusinessRules"); // 02/25/16 Igor TP#31379
			pStmt->GetString("CustomDrugRestrictionSP", blk.DrugRestrictionCustomSP, sizeof(blk.DrugRestrictionCustomSP));// 06/09/16 Igor TP#32254
			blk.ForceZeroProcessingFee = pStmt->GetInt("ForceZeroProcessingFee"); // 07/11/16 Igor TP#32459
			blk.LiftAndShiftRules = pStmt->GetInt("LiftAndShiftRules"); // 08/22/16 Igor TP#32667
			pStmt->GetString("CustomLNSCardValidationSP", blk.LNSCardValidationCustomSP, sizeof(blk.LNSCardValidationCustomSP)); // 08/22/16 Igor TP#32667
			blk.DebitRxProcessor = pStmt->GetInt("DebitRxProcessor"); // 10/13/16 Igor TP#32771
			blk.InsuredStatusOverrideRule = pStmt->GetInt("InsuredStatusOverrideRule");  // 11/10/16 Igor TP#33072
			pStmt->GetString("CostLimitOCC", blk.CostLimitOCC, sizeof(blk.CostLimitOCC)); // 08/16/17 Igor TP#35852
			blk.DebitCardFundingRule = pStmt->GetInt("DebitCardFundingRule"); // 06/22/17 Igor TP#35136
			blk.DebitCardFundsExpirationRules = pStmt->GetInt("DebitCardFundsExpirationRule"); // 07/25/17 Igor TP#35141
			blk.DebitCardFundsExpirationHours = pStmt->GetInt("DebitCardFundsExpirationHours"); // 07/25/17 Igor TP#35141
			blk.DebitCardFundsExpirationDays = pStmt->GetInt("DebitCardFundsExpirationDays"); // 07/25/17 Igor TP#35141
			blk.DebitCardFundsExpirationLapsedRefillDays = pStmt->GetInt("DebitCardFundsExpirationLapsedRefillDays"); // 07/25/17 Igor TP#35141
			blk.ManualClaimVerificationRules = pStmt->GetInt("ManualClaimVerificationRules"); // 08/14/17 Igor TP#35085
			blk.DebitDelayedProcessHour = pStmt->GetInt("DebitDelayedProcessHour");				// 08/22/17 Igor TP#36003
			blk.DebitDelayedProcessMinute = pStmt->GetInt("DebitDelayedProcessMinute");			// 08/22/17 Igor TP#36003
			blk.LogicalGroupIdentifier = pStmt->GetLong("LogicalGroupIdentifier");		// 11/14/17 Igor TP#36899
			blk.BenefitLookbackDays = pStmt->GetInt("BenefitLookbackDays");   // 10/26/17 Igor TP#36642
			blk.BenefitLookbackCap = pStmt->GetNumeric("BenefitLookbackCap"); // 10/26/17 Igor TP#36642
			blk.AdditionalMaximumBenefitIndividual = pStmt->GetNumeric("Additional_Maximum_Benefit_Individual");	// 03/23/2018 Igor TP#37951
			pStmt->GetString("Additional_Maximum_Benefit_Individual_Status_Codes", blk.AdditionalMaximumBenefitIndividualStatusCodes, sizeof(blk.AdditionalMaximumBenefitIndividualStatusCodes)); // 03/23/2018 Igor TP#37951
			blk.TokenBenefitRule = pStmt->GetNumeric("TokenBenefitRule");	// 03/09/18 Igor TP#37818		
			blk.TokenBenefitAmountCash = pStmt->GetNumeric("TokenBenefitAmountCash");	// 03/09/18 Igor TP#37818			
			blk.TokenBenefitAmountInsured = pStmt->GetNumeric("TokenBenefitAmountInsured");	// 03/09/18 Igor TP#37818			
			blk.TokenBenefitAmountInsNotCovered = pStmt->GetNumeric("TokenBenefitAmountInsNotCovered");	// 03/09/18 Igor TP#37818		
			blk.MailTokenBenefitAmountCash = pStmt->GetNumeric("MailTokenBenefitAmountCash");	// 03/09/18 Igor TP#37818
			blk.MailTokenBenefitAmountInsured = pStmt->GetNumeric("MailTokenBenefitAmountInsured");	// 03/09/18 Igor TP#37818
			blk.MailTokenBenefitAmountInsNotCovered = pStmt->GetNumeric("MailTokenBenefitAmountInsNotCovered");	// 03/09/18 Igor TP#37818
			blk.SpecialtyTokenBenefitAmountCash = pStmt->GetNumeric("SpecialtyTokenBenefitAmountCash");	// 03/09/18 Igor TP#37818
			blk.SpecialtyTokenBenefitAmountInsured = pStmt->GetNumeric("SpecialtyTokenBenefitAmountInsured");	// 03/09/18 Igor TP#37818
			blk.SpecialtyTokenBenefitAmountInsNotCovered = pStmt->GetNumeric("SpecialtyTokenBenefitAmountInsNotCovered");	// 03/09/18 Igor TP#37818
			blk.TokenBenefitPercentageCash = pStmt->GetNumeric("TokenBenefitPercentageCash");	// 03/09/18 Igor TP#37818
			blk.TokenBenefitPercentageInsured = pStmt->GetNumeric("TokenBenefitPercentageInsured");	// 03/09/18 Igor TP#37818
			blk.TokenBenefitPercentageInsNotCovered = pStmt->GetNumeric("TokenBenefitPercentageInsNotCovered");	// 03/09/18 Igor TP#37818
			blk.MailTokenBenefitPercentageCash = pStmt->GetNumeric("MailTokenBenefitPercentageCash");	// 03/09/18 Igor TP#37818
			blk.MailTokenBenefitPercentageInsured = pStmt->GetNumeric("MailTokenBenefitPercentageInsured");	// 03/09/18 Igor TP#37818
			blk.MailTokenBenefitPercentageInsNotCovered = pStmt->GetNumeric("MailTokenBenefitPercentageInsNotCovered");	// 03/09/18 Igor TP#37818
			blk.SpecialtyTokenBenefitPercentageCash = pStmt->GetNumeric("SpecialtyTokenBenefitPercentageCash");	// 03/09/18 Igor TP#37818
			blk.SpecialtyTokenBenefitPercentageInsured = pStmt->GetNumeric("SpecialtyTokenBenefitPercentageInsured");	// 03/09/18 Igor TP#37818
			blk.SpecialtyTokenBenefitPercentageInsNotCovered = pStmt->GetNumeric("SpecialtyTokenBenefitPercentageInsNotCovered");	// 03/09/18 Igor TP#37818
			blk.MaxBenefitIndividualWarningMsgThreshold = pStmt->GetNumeric("MaxBenefitIndividualWarningMsgThreshold"); // 04/26/18 Igor TP#38114
			blk.VoidReswitchTimeout = pStmt->GetNumeric("VoidReswitchTimeout"); // 05/28/18 Igor TP#38663
			// 02/16/21 Igor TP#??? - DirectSubmission should be retrieved separately before any overrides 
			//blk.DirectSubmissionRule = pStmt->GetInt("DirectSubmissionRule");   // 07/24/18 Igor TP#39088
			pStmt->GetString("BlockedPatientGenders", blk.BlockedPatientGenders, sizeof(blk.BlockedPatientGenders)); // 11/14/18 Igor TP#39872
			blk.DebitRxPlanSubType = pStmt->GetInt("DebitRxPlanSubType"); // 11/28/18 Igor TP#39961
			blk.TokenBenefitMultiplierValue = pStmt->GetInt("TokenBenefitMultiplierValue"); // 03/10/19 Igor TP#40421 
			blk.TokenBenefitDollarDigitCount = pStmt->GetInt("TokenBenefitDollarDigitCount"); // 03/10/19 Igor TP#40421 
			// 04/05/19 Igor TP#40761 
			blk.MedicaidVerificationRule = pStmt->GetInt("MedicaidVerificationRule");
			blk.MedicaidVerificationLookbackPeriod = pStmt->GetInt("MedicaidVerificationLookbackPeriod");
			blk.MedicaidVerificationUniquePARequired = pStmt->GetInt("MedicaidVerificationUniquePARequired");
			blk.TricareVerificationRule = pStmt->GetInt("TricareVerificationRule");
			blk.TricareVerificationLookbackPeriod = pStmt->GetInt("TricareVerificationLookbackPeriod");
			blk.TricareVerificationUniquePARequired = pStmt->GetInt("TricareVerificationUniquePARequired");
			blk.TokenBenefitMinimumStartingAmountCash = pStmt->GetNumeric("TokenBenefitMinimumStartingAmountCash", -1); // 05/22/19 Igor TP#41029
			blk.TokenBenefitMinimumStartingAmountInsured = pStmt->GetNumeric("TokenBenefitMinimumStartingAmountInsured", -1); // 05/22/19 Igor TP#41029
			blk.TokenBenefitMinimumStartingAmountInsNotCovered = pStmt->GetNumeric("TokenBenefitMinimumStartingAmountInsNotCovered", -1); // 05/22/19 Igor TP#41029
			blk.MailTokenBenefitMinimumStartingAmountCash = pStmt->GetNumeric("MailTokenBenefitMinimumStartingAmountCash", -1); // 05/22/19 Igor TP#41029
			blk.MailTokenBenefitMinimumStartingAmountInsured = pStmt->GetNumeric("MailTokenBenefitMinimumStartingAmountInsured", -1); // 05/22/19 Igor TP#41029
			blk.MailTokenBenefitMinimumStartingAmountInsNotCovered = pStmt->GetNumeric("MailTokenBenefitMinimumStartingAmountInsNotCovered", -1); // 05/22/19 Igor TP#41029
			blk.SpecialtyTokenBenefitMinimumStartingAmountCash = pStmt->GetNumeric("SpecialtyTokenBenefitMinimumStartingAmountCash", -1); // 05/22/19 Igor TP#41029
			blk.SpecialtyTokenBenefitMinimumStartingAmountInsured = pStmt->GetNumeric("SpecialtyTokenBenefitMinimumStartingAmountInsured", -1); // 05/22/19 Igor TP#41029
			blk.SpecialtyTokenBenefitMinimumStartingAmountInsNotCovered = pStmt->GetNumeric("SpecialtyTokenBenefitMinimumStartingAmountInsNotCovered", -1); // 05/22/19 Igor TP#41029
			blk.TokenBenefitMaximumStartingAmountCash = pStmt->GetNumeric("TokenBenefitMaximumStartingAmountCash", -1); // 05/22/19 Igor TP#41029
			blk.TokenBenefitMaximumStartingAmountInsured = pStmt->GetNumeric("TokenBenefitMaximumStartingAmountInsured", -1); // 05/22/19 Igor TP#41029
			blk.TokenBenefitMaximumStartingAmountInsNotCovered = pStmt->GetNumeric("TokenBenefitMaximumStartingAmountInsNotCovered", -1); // 05/22/19 Igor TP#41029
			blk.MailTokenBenefitMaximumStartingAmountCash = pStmt->GetNumeric("MailTokenBenefitMaximumStartingAmountCash", -1); // 05/22/19 Igor TP#41029
			blk.MailTokenBenefitMaximumStartingAmountInsured = pStmt->GetNumeric("MailTokenBenefitMaximumStartingAmountInsured", -1); // 05/22/19 Igor TP#41029
			blk.MailTokenBenefitMaximumStartingAmountInsNotCovered = pStmt->GetNumeric("MailTokenBenefitMaximumStartingAmountInsNotCovered", -1); // 05/22/19 Igor TP#41029
			blk.SpecialtyTokenBenefitMaximumStartingAmountCash = pStmt->GetNumeric("SpecialtyTokenBenefitMaximumStartingAmountCash", -1); // 05/22/19 Igor TP#41029
			blk.SpecialtyTokenBenefitMaximumStartingAmountInsured = pStmt->GetNumeric("SpecialtyTokenBenefitMaximumStartingAmountInsured", -1); // 05/22/19 Igor TP#41029
			blk.SpecialtyTokenBenefitMaximumStartingAmountInsNotCovered = pStmt->GetNumeric("SpecialtyTokenBenefitMaximumStartingAmountInsNotCovered", -1); // 05/22/19 Igor TP#41029
			blk.PreloadLookBackDays = pStmt->GetInt("PreloadLookBackDays"); // 01/16/20 Igor TP#42088
			blk.FlexDupeClaimCheckRule = pStmt->GetInt("FlexDupeClaimCheckRule"); // 02/26/20 Igor TP#43096
			blk.PriorAuthTransferRule = pStmt->GetInt("PriorAuthTransferRule"); // 10/09/20 Igor TP#44765
			blk.PriorAuthTransferDays = pStmt->GetInt("PriorAuthTransferDays", -1); // 10/09/20 Igor TP#44765
			pStmt->GetString("CustomMaxBenefitIndividualSP", blk.CustomMaxBenefitIndividualSP, sizeof(blk.CustomMaxBenefitIndividualSP)); // 10/30/20 Troy TP#45555 - ability to call a custom SP for determining Annual Cap
			blk.MaximizerRule = pStmt->GetInt("MaximizerRule"); // 11/19/20 Igor TP#45711
			blk.MaximizerMaximumBenefitIndividual = pStmt->GetNumeric("MaximizerMaximumBenefitIndividual", -1);
			blk.DebitCardLoadSubmittedPPAThreshold = pStmt->GetNumeric("DebitCardLoadSubmittedPPAThreshold"); // 01/25/21 Igor TP#46377
			pStmt->GetString("CustomNextFillAllowedAfterSP", blk.CustomNextFillAllowedAfterSP, sizeof(blk.CustomNextFillAllowedAfterSP)); // 03/18/21 Igor TP#46417
			pStmt->GetDatePackedString("EarliestSubmissionDate", blk.EarliestSubmissionDate, sizeof(blk.EarliestSubmissionDate)); // 07/08/21 Igor TP#45618
			blk.AdjustFundAmountByCardBalance = pStmt->GetInt("AdjustFundAmountByCardBalance", NULL_VALUE); // 09/03/21 Igor TP#48612
			blk.AdditionalMaximumBenefitIndividualRule = pStmt->GetInt("AdditionalMaximumBenefitIndividualRule", blk.AdditionalMaximumBenefitIndividualRule);
			blk.MaximizerDispensingThreshold = pStmt->GetInt("MaximizerDispensingThreshold", blk.MaximizerDispensingThreshold); // 12/16/21 Igor TP#49256
			blk.ExpandedMaximumBenefitIndividual = pStmt->GetNumeric("ExpandedMaximumBenefitIndividual", -1);// 12/23/21 Igor TP#49267
			blk.AdditionalExpandedMaximumBenefitIndividual = pStmt->GetNumeric("AdditionalExpandedMaximumBenefitIndividual", NULL_VALUE); // 01/31/22 Igor TP#49388
			pStmt->GetString("AdditionalExpandedMaximumBenefitIndividualStatusCodes", blk.AdditionalExpMaxBenefitIndividualStatusCodes, sizeof(blk.AdditionalExpMaxBenefitIndividualStatusCodes), blk.AdditionalExpMaxBenefitIndividualStatusCodes); // 01/31/22 Igor TP#49388
			blk.MaxDispensingsPerCalendarYear = pStmt->GetNumeric("MaxDispensingsPerCalendarYear", NULL_VALUE); // 03/08/22 Igor TP#49063
			pStmt->GetString("MaximizerOtherCoverageCodes", blk.MaximizerOtherCoverageCodes, sizeof(blk.MaximizerOtherCoverageCodes), blk.MaximizerOtherCoverageCodes); // 04/27/22 Igor TP#49787
			blk.TieredAnnualCapRule = pStmt->GetInt("TieredAnnualCapRule"); // 12/05/22 Troy OCLE-4639
			blk.Tier1MaximumBenefitIndividual = pStmt->GetNumeric("Tier1MaximumBenefitIndividual", -1); // 12/05/22 Troy OCLE-4639
			blk.Tier1AnnualCapDispensingThreshold = pStmt->GetInt("Tier1AnnualCapDispensingThreshold", blk.Tier1AnnualCapDispensingThreshold); // 12/05/22 Troy OCLE-4639
			pStmt->GetChar("Tier1MaxIndividBenefitExceededBehavior", &blk.Tier1MaxIndividBenefitExceededBehavior, blk.Tier1MaxIndividBenefitExceededBehavior); // 03/14/23 Igor OCLE-4751
			pStmt->GetChar("ExpandedMaxIndividBenefitExceededBehavior", &blk.ExpandedMaxIndividBenefitExceededBehavior, blk.ExpandedMaxIndividBenefitExceededBehavior); // 04/07/23 Troy OCLE-4841
			blk.ExpandedMaximumBenefitIndividualPriorAuthRule = pStmt->GetInt("ExpandedMaximumBenefitIndividualPriorAuthRule", blk.ExpandedMaximumBenefitIndividualPriorAuthRule); // 04/07/23 Troy OCLE-4844
			/////////////////////////////////////////////
			blk.BenefitDeterminationMethod = BDM_PLAN; // 01/21/14 Igor TP#24803

			if (pStmt->Next())
			{
				reporter.error("Returned more than 1 record");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}
		}
		else
		{
			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return UniqueID;
}

// 01/03/13 Igor TP#22977 - re-wrote the function GetUniqueControlID
long GetProviderControlID(long PlanID, long ProviderID, char* Chain_Code)
{
	Reporter reporter(MonitorFile, "GetProviderControlID");

	char szStmt[1024];
	int UniqueID = 0;

	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		strcpy_s(szStmt, sizeof(szStmt), "SELECT Unique_ID"
			" FROM CLAIMS.M_Provider_Control"
			" WITH (NOLOCK)" // 03/25/14 Igor TP#25182
			" WHERE Control_ID=? AND Provider_ID=?");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(PlanID);
		pStmt->SetInt(ProviderID);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (pStmt->First())
			UniqueID = pStmt->GetLong("Unique_ID");
		else if (Chain_Code)
		{
			pStmt = gOdbcManager.GetStatement(true);

			strcpy_s(szStmt, sizeof(szStmt), "SELECT Unique_ID"
				" FROM CLAIMS.M_Provider_Control"
				" WITH (NOLOCK)" // 03/25/14 Igor TP#25182
				" WHERE Control_ID=? AND Chain_Code=?");

			pStmt->Prepare(szStmt);

			if (!pStmt->BindParamsAuto())
			{
				reporter.error("Query contains no parameters");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			pStmt->SetInt(PlanID);
			pStmt->SetCharPtr(Chain_Code);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->ExecutePrepared();

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
				UniqueID = pStmt->GetLong("Unique_ID");
			else
			{
				gOdbcManager.ReleaseStatement();
				return RETURN_FAILURE;
			}
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return UniqueID;
}

double AccumlatedBenefitsForPillsBasedDispensing(long ClaimantID, double Dispensing) // 07/25/11 Igor - Changed to Double
{
	Reporter reporter(MonitorFile, "AccumlatedBenefitsForPillsBasedDispensing");

	char szStmt[1024];
	double CouponAmount = 0;

	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		sprintf_s(szStmt, sizeof(szStmt), "SELECT SUM(");

		/*
		// 10/18/16 Igor TP#32997 - added condition for DebitRx
		if (Group_blk.DebitRxPlan == 'Y')
			strcat_s(szStmt, sizeof(szStmt), "ISNULL(ca.DebitCardLoadAmount,ca.CouponAmt)");
		else // DebitRxPlan == 0 or DebitRxPlan == 'H'
			strcat_s(szStmt, sizeof(szStmt), "ca.CouponAmt");
		*/
		// 02/19/20 Igor TP#42955 - trying to use DebitCardLoadAmount as previous claims could be processed as a check payment or electronic
		strcat_s(szStmt, sizeof(szStmt), "CASE WHEN ISNULL(ca.DebitCardLoadAmount,0)=0 THEN ca.CouponAmt ELSE ca.DebitCardLoadAmount END");
		strcat_s(szStmt, sizeof(szStmt), "+ISNULL(ca.ProviderDirectPayBenefitAmount,0)"); // 03/20/20 Igor TP#43523
		strcat_s(szStmt, sizeof(szStmt), ")"
			" FROM CLAIMS.M_Active a"
			" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
			" INNER JOIN CLAIMS.M_ClaimAmounts ca"
			" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
			" ON a.Unique_ID = ca.Activity_ID"
			" WHERE a.Claimant_ID=? AND a.NumDispensings=?");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(ClaimantID);
		pStmt->SetNumeric(Dispensing);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (pStmt->First())
		{
			CouponAmount = pStmt->GetNumeric(0);

			if (pStmt->Next())
			{
				reporter.error("Returned more than 1 record");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			// 09/30/19 Igor TP#41655 - consider debit card claw-backs
			// if (Group_blk.DebitRxPlan == 'H') // 12/06/21 Igor TP#4921 - have to consider all previous claims
			pStmt = gOdbcManager.GetStatement(true);

			strcpy_s(szStmt, sizeof(szStmt), "SELECT SUM(DRT.Amount)"
				" FROM CLAIMS.DebitRxNonClaimEvents DRNCE WITH (NOLOCK)"
				" INNER JOIN CLAIMS.DebitRxTransactions DRT WITH (NOLOCK) ON DRNCE.DebitRxNonClaimEventID = DRT.ActivityID AND DRT.ActivityType = 'O'"
				" WHERE DRNCE.EventTypeID = 2 AND DRNCE.MActiveUniqueID IN"
				" (SELECT Unique_ID FROM CLAIMS.M_Active WHERE a.Claimant_ID=? AND a.NumDispensings=?)");

			pStmt->Prepare(szStmt);

			if (!pStmt->BindParamsAuto())
			{
				reporter.error("Query contains no parameters");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			pStmt->SetInt(ClaimantID);
			pStmt->SetNumeric(Dispensing);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->ExecutePrepared();

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
			{
				double TotalClawbackAmount = pStmt->GetNumeric(0);

				if (TotalClawbackAmount > 0)
				{
					reporter.info("Considering unused clawback amount of <%.2f>", TotalClawbackAmount);
					CouponAmount -= TotalClawbackAmount;
				}
			}
		}
		else
		{
			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return CouponAmount;
}

// 07/26/13 Igor TP#23982 - removed code for non CoPayReduction (using ProviderReimb), removed CoPayReduction parameter
// 01/29/14 Igor TP#24844 - re-wrote the function to remove duplicated code, changed dates calculation, added CPL_Rule 6 and MaxBenefitIndividualRule 5 and 6 logic  
// 08/21/17 Igor TP#35992 - returning negative benefit was causing an error, added benefit as parameter
int AccumulatedBenefits(double &Benefit, Claimant_Info &Claimant_blk, RX_Message_struct* pRX_Message, BOOL Individual_Only)
{
	Reporter reporter(MonitorFile, "AccumulatedBenefits");

	char szStmt[1024] = { 0 };
	char buff[1000];

	char BenefitStartDate[PACKED_DATE_SIZE] = {0}, BenefitEndDate[PACKED_DATE_SIZE] = {0};

	Benefit = 0;

	OdbcStatement *pStmt = NULL;

	try
	{
		if (Group_blk.MaxBenefitIndividualRule == 1 || Group_blk.MaxBenefitIndividualRule == 3)
		{
			sprintf_s(BenefitStartDate, sizeof(BenefitStartDate), "%4.4s0101", pRX_Message->DateFilled);
		}
		else if (Group_blk.MaxBenefitIndividualRule == 2 || Group_blk.MaxBenefitIndividualRule == 4) // 12/05/13 Igor TP#24469
		{
			pStmt = gOdbcManager.GetStatement();

			if (Group_blk.UseHistoricalData)
				strcat_s(szStmt, sizeof(szStmt), "SELECT TOP 1 * FROM ("); // * selects all fields from sub-selects with Date_Filled only

			// 03/11/16 Igor TP#31280 - added history tables
			for (int i = 0; i <= Group_blk.UseHistoricalData; i++)
			{
				if (i)
					strcat_s(szStmt, sizeof(szStmt), " UNION ");

				strcat_s(szStmt, sizeof(szStmt), "SELECT");
				if (!Group_blk.UseHistoricalData)
					strcat_s(szStmt, sizeof(szStmt), " TOP 1");
				strcat_s(szStmt, sizeof(szStmt), " Date_Filled");

				if (i)
					strcat_s(szStmt, sizeof(szStmt), " FROM CLAIMS.HistoricalPaidClaims WITH (NOLOCK, INDEX (idx_HistPaid_InsLookback))"); // 11/06/18 Igor TP#39850  - added INDEX
				else
					strcat_s(szStmt, sizeof(szStmt), " FROM CLAIMS.M_Active WITH (NOLOCK, INDEX (InsLookbackIdx))");

				strcat_s(szStmt, sizeof(szStmt), " WHERE Claimant_ID");

				// 02/06/14 Igor TP#24825 - added CPL_Rule 6 logic
				if (Individual_Only && (Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS
					|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS)) // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
					sprintf_s(buff, sizeof(buff), " IN (SELECT Unique_ID FROM CLAIMS.M_Claimant"
						" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
						" WHERE"
						//" LEFT(Member_Number,3)<>'DEL'"
						" CHARINDEX('DEL',Member_Number)=0" // 07/09/15 Igor TP#29712 - replaced LEFT with CHARINDEX
						" AND MasterClaimantID=%ld)", Claimant_blk.MasterClaimantID); // 02/24/14 Igor TP#25091 - using MasterClaimantID instead of Unique_ID
				else
					sprintf_s(buff, sizeof(buff), "=%ld", Claimant_blk.Unique_ID);

				strcat_s(szStmt, sizeof(szStmt), buff);
			}

			if (Group_blk.UseHistoricalData)
				strcat_s(szStmt, sizeof(szStmt), ") Res");

			strcat_s(szStmt, sizeof(szStmt), " ORDER BY Date_Filled");

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->Execute(szStmt);

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
			{
				pStmt->GetDatePackedString("Date_Filled", BenefitStartDate, sizeof(BenefitStartDate));
			}
		}
		else if (Group_blk.MaxBenefitIndividualRule == 5 || Group_blk.MaxBenefitIndividualRule == 6) // 01/29/14 Igor TP#24844
		{
			// 02/06/14 Igor TP#24825 - added CPL_Rule 6 logic
			if (Individual_Only && (Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS
				|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS)) // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
			{
				pStmt = gOdbcManager.GetStatement();

				strcpy_s(szStmt, sizeof(szStmt), "SELECT TOP 1 BenefitStartDate"
					" FROM CLAIMS.M_Claimant"
					" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
					" WHERE"
					//" LEFT(Member_Number,3)<>'DEL'"
					" CHARINDEX('DEL',Member_Number)=0" // 07/09/15 Igor TP#29712 - replaced LEFT with CHARINDEX
					" AND MasterClaimantID=?"
					" ORDER BY BenefitStartDate");

				pStmt->Prepare(szStmt);

				if (!pStmt->BindParamsAuto())
				{
					reporter.error("Query contains no parameters");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				pStmt->SetInt(Claimant_blk.MasterClaimantID);

				reporter.timedStart(pStmt->PrintSql(szStmt));

				pStmt->ExecutePrepared();

				reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

				if (!pStmt->BindAuto())
				{
					reporter.error("Cursor doesn't contain resultset");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if (pStmt->First())
				{
					pStmt->GetDatePackedString("BenefitStartDate", BenefitStartDate, sizeof(BenefitStartDate));
				}
			}
			else
			{
				if (!Claimant_blk.BenefitStartDate[0])
				{
					reporter.warn("Invalid patient's BenefitStartDate (MaxBenefitIndividualRule <%d>)", Group_blk.MaxBenefitIndividualRule);
					gOdbcManager.ReleaseStatement();
					return RETURN_FAILURE;
				}
				strcpy_s(BenefitStartDate, sizeof(BenefitStartDate), Claimant_blk.BenefitStartDate);
			}
		}
		// 03/31/14 Igor TP#25223 - added missing "else" below
		else if (Group_blk.MaxBenefitIndividualRule == 7
			|| Group_blk.MaxBenefitIndividualRule == 8 // 03/14/14 Igor TP#25144
			)
		{
			//if (Group_blk.EnrollmentDays)
			if (Group_blk.ProcessEnrollment) // 10/14/20 Igor TP#44788 - added ProcessEnrollment flag
			{
				pStmt = gOdbcManager.GetStatement();

				int EndingMonth = 0;
				long PlanID = 0; // Plan_ID from PatientBenefitPeriods may be different from current Group_blk.UniqueID if using linked groups

				// 1. Find the group id and last month from BenefitRulesAcrossTimePeriod based on current record in PatientBenefitPeriods

				strcpy_s(szStmt, sizeof(szStmt),
					//"SELECT c.Plan_ID, b.EndingMonth" 
					"SELECT b.PlanID, b.EndingMonth" // 09/22/15 Igor - using PlanID from BenefitRulesAcrossTimePeriod
					" FROM CLAIMS.PatientBenefitPeriods a"
					" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
					" INNER JOIN CLAIMS.BenefitRulesAcrossTimePeriod b WITH (NOLOCK) ON b.BenefitRulesAcrossTimePeriodID = a.BenefitRulesAcrossTimePeriodID"
					// 09/22/15 Igor - using PlanID from BenefitRulesAcrossTimePeriod
					//" INNER JOIN CLAIMS.m_Claimant c"
					//" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
					//" ON c.Unique_ID=a.ClaimantID"
					" WHERE a.ClaimantID=?"
					" AND ? BETWEEN a.StartDate AND a.EndDate");

				pStmt->Prepare(szStmt);

				if (!pStmt->BindParamsAuto())
				{
					reporter.error("Query contains no parameters");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if ((Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS
					|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS) // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
					&& Claimant_blk.MasterClaimantID)
					pStmt->SetInt(Claimant_blk.MasterClaimantID);
				else
					pStmt->SetInt(Claimant_blk.Unique_ID);
				pStmt->SetDateString(pRX_Message->DateFilled);

				reporter.timedStart(pStmt->PrintSql(szStmt));

				pStmt->ExecutePrepared();

				reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

				if (!pStmt->BindAuto())
				{
					reporter.error("Cursor doesn't contain resultset");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if (pStmt->First())
				{
					//PlanID = pStmt->GetInt("Plan_ID");
					// 09/22/15 Igor - using PlanID from BenefitRulesAcrossTimePeriod
					PlanID = pStmt->GetInt("PlanID");
					EndingMonth = pStmt->GetInt("EndingMonth");
				}
				// 05/14/15 Igor TP#29185 - commented out "else" block
				//else
				//{
				//	reporter.debug("No records found");
				//	gOdbcManager.ReleaseStatement();
				//	return RETURN_FAILURE;
				//}

				if (!EndingMonth)
				{
					reporter.info("Cannot get Accumulated Benefit for %sClaimantID <%ld> - PatientBenefitPeriods record not found",
						Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? "Master" : "", // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
						Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? Claimant_blk.MasterClaimantID : Claimant_blk.Unique_ID);
					gOdbcManager.ReleaseStatement();
					return RETURN_FAILURE;
				}

				int StartingMonth = 0;

				// 2. Find starting and ending month from MaximumBenefitIndividualControl

				pStmt = gOdbcManager.GetStatement(true);

				strcpy_s(szStmt, sizeof(szStmt),
					"SELECT StartingMonth,EndingMonth"
					" FROM CLAIMS.MaximumBenefitIndividualControl WITH (NOLOCK)"
					" WHERE PlanID=? AND ? BETWEEN StartingMonth AND EndingMonth");

				pStmt->Prepare(szStmt);

				if (!pStmt->BindParamsAuto())
				{
					reporter.error("Query contains no parameters");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				pStmt->SetInt(PlanID);
				pStmt->SetInt(EndingMonth);

				reporter.timedStart(pStmt->PrintSql(szStmt));

				pStmt->ExecutePrepared();

				reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

				if (!pStmt->BindAuto())
				{
					reporter.error("Cursor doesn't contain resultset");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if (pStmt->First())
				{
					StartingMonth = pStmt->GetInt("StartingMonth");
					EndingMonth = pStmt->GetInt("EndingMonth");
				}
				// 05/14/15 Igor TP#29185 - commented out "else" block
				//else
				//{
				//	reporter.debug("No records found");
				//	gOdbcManager.ReleaseStatement();
				//	return RETURN_FAILURE;
				//}

				if (!StartingMonth)
				{
					reporter.info("Cannot get Accumulated Benefit for %sClaimantID <%ld> - MaximumBenefitIndividualControl record not found",
						Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? "Master" : "", // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
						Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? Claimant_blk.MasterClaimantID : Claimant_blk.Unique_ID);
					gOdbcManager.ReleaseStatement();
					return RETURN_FAILURE;
				}

				// 3. Determine StartDate

				pStmt = gOdbcManager.GetStatement(true);

				strcpy_s(szStmt, sizeof(szStmt),
					"SELECT TOP 1 StartDate"
					" FROM CLAIMS.PatientBenefitPeriods PBP"
					" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
					" INNER JOIN CLAIMS.BenefitRulesAcrossTimePeriod TP WITH (NOLOCK) ON PBP.BenefitRulesAcrossTimePeriodID=TP.BenefitRulesAcrossTimePeriodID"
					//" WHERE ClaimantID=? AND StartingMonth=?");
					// 05/14/15 Igor TP#29185 - adjusted WHERE clause and added TOP 1 restriction on ordered result
					" WHERE ClaimantID=? AND StartingMonth>=?"
					" ORDER BY StartingMonth");

				pStmt->Prepare(szStmt);

				if (!pStmt->BindParamsAuto())
				{
					reporter.error("Query contains no parameters");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if ((Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS
					|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS) // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
					&& Claimant_blk.MasterClaimantID)
					pStmt->SetInt(Claimant_blk.MasterClaimantID);
				else
					pStmt->SetInt(Claimant_blk.Unique_ID);
				pStmt->SetInt(StartingMonth);

				reporter.timedStart(pStmt->PrintSql(szStmt));

				pStmt->ExecutePrepared();

				reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

				if (!pStmt->BindAuto())
				{
					reporter.error("Cursor doesn't contain resultset");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if (pStmt->First())
				{
					pStmt->GetDatePackedString(0, BenefitStartDate, sizeof(BenefitStartDate));
				}
				// 05/14/15 Igor TP#29185 - commented out "else" block and added "if" check
				//else
				//{
				//	reporter.debug("No records found");
				//	gOdbcManager.ReleaseStatement();
				//	return RETURN_FAILURE;
				//}
				if (BenefitStartDate[0])
				{
					// 4. Determine EndDate

					pStmt = gOdbcManager.GetStatement(true);

					strcpy_s(szStmt, sizeof(szStmt),
						"SELECT TOP 1 EndDate"
						" FROM CLAIMS.PatientBenefitPeriods PBP"
						" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
						" INNER JOIN CLAIMS.BenefitRulesAcrossTimePeriod TP WITH (NOLOCK) ON PBP.BenefitRulesAcrossTimePeriodID=TP.BenefitRulesAcrossTimePeriodID"
						" WHERE ClaimantID=?"
						" AND EndingMonth<=?"
						" ORDER BY EndingMonth DESC");

					pStmt->Prepare(szStmt);

					if (!pStmt->BindParamsAuto())
					{
						reporter.error("Query contains no parameters");
						gOdbcManager.ReleaseStatement(true);
						return SQL_PROCESSING_ERROR;
					}

					if ((Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS
						|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS) // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
						&& Claimant_blk.MasterClaimantID)
						pStmt->SetInt(Claimant_blk.MasterClaimantID);
					else
						pStmt->SetInt(Claimant_blk.Unique_ID);
					pStmt->SetInt(EndingMonth);

					reporter.timedStart(pStmt->PrintSql(szStmt));

					pStmt->ExecutePrepared();

					reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

					if (!pStmt->BindAuto())
					{
						reporter.error("Cursor doesn't contain resultset");
						gOdbcManager.ReleaseStatement(true);
						return SQL_PROCESSING_ERROR;
					}

					if (pStmt->First())
					{
						pStmt->GetDatePackedString(0, BenefitEndDate, sizeof(BenefitEndDate));
					}
					// 05/14/15 Igor TP#29185 - commented out "else" block
					//else
					//{
					//	reporter.debug("No records found");
					//	gOdbcManager.ReleaseStatement();
					//	return RETURN_FAILURE;
					//}
				}
			}
			else
				reporter.warn("EnrollmentDays not set in M_Plan");
		}
		else if (Group_blk.MaxBenefitIndividualRule != 9 // 09/10/15 Igor TP#29994
			&& Group_blk.MaxBenefitIndividualRule != 10 && Group_blk.MaxBenefitIndividualRule != 11)// 10/30/20 Troy TP#45555 - Added Rules 10 and 11
		{
			reporter.info("Cannot get Accumulated Benefit (Invalid MaxBenefitIndividualRule <%d>)", Group_blk.MaxBenefitIndividualRule);
			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}

		if (Group_blk.MaxBenefitIndividualRule != 9 && // 09/10/15 Igor TP#29994
			Group_blk.MaxBenefitIndividualRule != 10 && Group_blk.MaxBenefitIndividualRule != 11 && // 10/30/20 Troy TP#45555 - Added Rules 10 and 11
			!BenefitStartDate[0])
		{
			reporter.info("Cannot get Accumulated Benefit for %sClaimantID <%ld> - No Prior Claims found",
				Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? "Master" : "", // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
				Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? Claimant_blk.MasterClaimantID : Claimant_blk.Unique_ID);

			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}

		// 04/11/16 Igor TP#31733 - The benefits should reset every 12 months
		if (Group_blk.MaxBenefitIndividualRule == 2 || Group_blk.MaxBenefitIndividualRule == 4 ||
			Group_blk.MaxBenefitIndividualRule == 5 || Group_blk.MaxBenefitIndividualRule == 6)
		{
			// calculate benefit anniversary date relative to BenefitStartDate and Fill Date
			char AnniversaryDate[PACKED_DATE_SIZE];
			DateFromDate(0, 0, 1, AnniversaryDate, sizeof(AnniversaryDate), BenefitStartDate);
			while (Date2Num(AnniversaryDate) <= Date2Num(pRX_Message->DateFilled))
			{
				strcpy_s(BenefitStartDate, sizeof(BenefitStartDate), AnniversaryDate);
				DateFromDate(0, 0, 1, AnniversaryDate, sizeof(AnniversaryDate), AnniversaryDate);
			}
		}

		// Determine BenefitEndDate

		if (Group_blk.MaxBenefitIndividualRule == 1 || Group_blk.MaxBenefitIndividualRule == 3)
		{
			sprintf_s(BenefitEndDate, sizeof(BenefitEndDate), "%4.4s1231", pRX_Message->DateFilled);
		}
		else if (Group_blk.MaxBenefitIndividualRule != 7 && Group_blk.MaxBenefitIndividualRule != 8 // 03/14/14 Igor TP#25144 - For rules 7 and 8 BenefitEndDate is already calculated
			&& Group_blk.MaxBenefitIndividualRule != 9) // 09/10/15 Igor TP#29994 - no need to calculate BenefitEndDate for rule 9
		{
			// 11/06/13 Igor TP#24415
			DateFromDate(-1, 0, 1, BenefitEndDate, sizeof(BenefitEndDate), BenefitStartDate); // 04/11/16 Igor TP#31733 - adjusted BenefitEndDate to be 1 day less
		}

		if (Group_blk.MaxBenefitIndividualRule != 9 && // 09/10/15 Igor TP#29994
			Group_blk.MaxBenefitIndividualRule != 10 && Group_blk.MaxBenefitIndividualRule != 11 && // 10/30/20 Troy TP#45555 - Added Rules 10 and 11
			!BenefitEndDate[0])
		{
			reporter.info("Cannot get Accumulated Benefit for %sClaimantID <%ld> - EndDate cannot be determined",
				Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? "Master" : "", // 02/05/16 Igor TP#31209 - added CPL_Rule 10 logic
				Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS ? Claimant_blk.MasterClaimantID : Claimant_blk.Unique_ID);

			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}

		double PreFundAmount = 0;

		// 10/21/16 Igor TP#32770
		if (Group_blk.DebitRxPlan == 'Y' &&
			(Group_blk.MaxBenefitIndividualRule == 1 || Group_blk.MaxBenefitIndividualRule == 3))
		{
			pStmt = gOdbcManager.GetStatement(true);

			// 06/20/17 Igor TP#35139
			strcpy_s(szStmt, sizeof(szStmt), "{CALL CLAIMS.usp_CE_GetDebitRxLoadAmount("
				"?," //@GroupNumber VARCHAR(15),
				"?," //@MemberNumber VARCHAR(24),
				"?," //@BenefitStartDate DATETIME,
				"?"  //@BenefitEndDate DATETIME
				")}");

			pStmt->Prepare(szStmt);

			if (!pStmt->BindParamsAuto())
			{
				reporter.error("Query contains no parameters");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			pStmt->SetCharPtr(Group_blk.Group_ID);
			pStmt->SetCharPtr(Claimant_blk.MemberNumber);
			pStmt->SetDateString(BenefitStartDate);
			pStmt->SetDateString(BenefitEndDate);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->ExecutePrepared();

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
				PreFundAmount = pStmt->GetNumeric(0);
		}

		// 10/30/20 Troy TP#45555
		if (Group_blk.MaxBenefitIndividualRule != 10 && Group_blk.MaxBenefitIndividualRule != 11)
		{
			// Constructing the main query now		
			strcpy_s(szStmt, sizeof(szStmt), "SELECT SUM(");

			/*
			// 10/18/16 Igor TP#32997 - added condition for DebitRx
			if (Group_blk.DebitRxPlan == 'Y')
				strcat_s(szStmt, sizeof(szStmt), "ISNULL(ca.DebitCardLoadAmount,ca.CouponAmt)");
			else // DebitRxPlan == 0 or DebitRxPlan == 'H'
				strcat_s(szStmt, sizeof(szStmt), "ca.CouponAmt");
			// 09/10/15 Igor TP#29994 - re-wrote query composing code
			*/
			// 02/19/20 Igor TP#42955 - trying to use DebitCardLoadAmount as previous claims could be processed as a check payment or electronic
			strcat_s(szStmt, sizeof(szStmt), "CASE WHEN ISNULL(ca.DebitCardLoadAmount,0)=0 THEN ca.CouponAmt ELSE ca.DebitCardLoadAmount END");
			strcat_s(szStmt, sizeof(szStmt), "+ISNULL(ca.ProviderDirectPayBenefitAmount,0)"); // 03/20/20 Igor TP#43523
			// 09/10/15 Igor TP#29994 - re-wrote query composing code
			strcat_s(szStmt, sizeof(szStmt), ")"
				" FROM CLAIMS.M_Active a WITH (NOLOCK)"
				" INNER JOIN CLAIMS.M_ClaimAmounts ca WITH (NOLOCK)"
				" ON a.Unique_ID=ca.Activity_ID");

			if (Individual_Only)
			{
				strcat_s(szStmt, sizeof(szStmt), " WHERE a.Claimant_ID");

				if (Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS // 02/06/14 Igor TP#24825 - added CPL_Rule 6 logic
					|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS) // 02/08/16 Igor TP#31209 - added CPL_Rule 10 logic
					sprintf_s(buff, sizeof(buff), " IN (SELECT Unique_ID FROM CLAIMS.M_Claimant WITH (NOLOCK)"
						" WHERE MasterClaimantID=%ld)", Claimant_blk.MasterClaimantID); // 02/24/14 Igor TP#25091 - using MasterClaimantID instead of Unique_ID
				else
					sprintf_s(buff, sizeof(buff), "=%ld", Claimant_blk.Unique_ID);

				strcat_s(szStmt, sizeof(szStmt), buff);
			}
			else //family
			{
				sprintf_s(buff, sizeof(buff), " INNER JOIN CLAIMS.M_Claimant c WITH (NOLOCK)"
					" ON a.Claimant_ID=c.Unique_ID"
					" WHERE c.Member_Number='%s' AND c.Plan_ID=a.Plan_ID",
					Claimant_blk.MemberNumber);

				strcat_s(szStmt, sizeof(szStmt), buff);
			}

			// 01/03/17 Igor TP#33498
			if (Group_blk.MaxBenefitIndividualByProduct)
			{
				sprintf_s(buff, sizeof(buff), " AND a.Service_ID IN (SELECT Service_ID FROM Claims.m_Product_Control"
					" WITH (NOLOCK)"
					" WHERE PlanProductID=%ld)", Group_blk.PlanProductID);

				strcat_s(szStmt, sizeof(szStmt), buff);

				if (Group_blk.CPL_Rule != LINKED_CLAIMANT_CARDS && Group_blk.CPL_Rule != MULTIPLE_CLAIMANT_CARDS)
				{
					sprintf_s(buff, sizeof(buff), " AND a.Plan_ID=%ld", Group_blk.Unique_ID);

					strcat_s(szStmt, sizeof(szStmt), buff);
				}
			}

			if (Group_blk.MaxBenefitIndividualRule != 9) // 09/10/15 Igor TP#29994 - MaxBenefitIndividualRule 9 to use M_Plan.Maximum_Benefit_Individual
			{
				sprintf_s(buff, sizeof(buff), " AND (a.Date_Filled BETWEEN '%s' AND '%s')", BenefitStartDate, BenefitEndDate);
				strcat_s(szStmt, sizeof(szStmt), buff);
			}

			pStmt = gOdbcManager.GetStatement(true);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->Execute(szStmt);

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
				Benefit = pStmt->GetNumeric(0);
			else
			{
				reporter.info("Cannot get Accumulated Benefit for Patient");
				gOdbcManager.ReleaseStatement();
				return RETURN_FAILURE;
			}

			// 03/15/14 Igor TP#25144 - added Historical claims
			if (Group_blk.UseHistoricalData && Individual_Only)
			{
				sprintf_s(szStmt, sizeof(szStmt), "SELECT SUM(");

				/*
				// 10/18/16 Igor TP#32997 - added condition for DebitRx
				if (Group_blk.DebitRxPlan == 'Y')
					strcat_s(szStmt, sizeof(szStmt), "ISNULL(ca.DebitCardLoadAmount,ca.CouponAmt)");
				else // DebitRxPlan == 0 or DebitRxPlan == 'H'
					strcat_s(szStmt, sizeof(szStmt), "ca.CouponAmt");
				*/
				// 02/19/20 Igor TP#42955 - trying to use DebitCardLoadAmount as previous claims could be processed as a check payment or electronic
				strcat_s(szStmt, sizeof(szStmt), "CASE WHEN ISNULL(ca.DebitCardLoadAmount,0)=0 THEN ca.CouponAmt ELSE ca.DebitCardLoadAmount END");

				sprintf_s(buff, sizeof(buff), ")"
					" FROM CLAIMS.HistoricalPaidClaims a"
					" WITH (NOLOCK, INDEX (idx_HistPaid_InsLookback))" // 03/27/14 Igor TP#25182 // 11/06/18 Igor TP#39850  - added INDEX
					" INNER JOIN CLAIMS.HistoricalPaidClaimAmounts ca WITH (NOLOCK) ON ca.Activity_ID = a.Unique_ID" // 03/27/14 Igor TP#25182 - added WITH (NOLOCK)
					" WHERE (a.Date_Filled BETWEEN '%s' AND '%s') AND a.Claimant_ID",
					BenefitStartDate, BenefitEndDate);
				strcat_s(szStmt, sizeof(szStmt), buff);

				if (Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS // 02/06/14 Igor TP#24825 - added CPL_Rule 6 logic
					|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS) // 02/08/16 Igor TP#31209 - added CPL_Rule 10 logic
					sprintf_s(buff, sizeof(buff), " IN (SELECT Unique_ID FROM CLAIMS.M_Claimant"
						" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
						" WHERE MasterClaimantID=%ld)", Claimant_blk.MasterClaimantID); // 02/24/14 Igor TP#25091 - using MasterClaimantID instead of Unique_ID
				else
					sprintf_s(buff, sizeof(buff), "=%ld", Claimant_blk.Unique_ID);
				strcat_s(szStmt, sizeof(szStmt), buff);

				pStmt = gOdbcManager.GetStatement(true);

				reporter.timedStart(pStmt->PrintSql(szStmt));

				pStmt->Execute(szStmt);

				reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

				if (!pStmt->BindAuto())
				{
					reporter.error("Cursor doesn't contain resultset");
					gOdbcManager.ReleaseStatement(true);
					return SQL_PROCESSING_ERROR;
				}

				if (pStmt->First())
					Benefit += pStmt->GetNumeric(0);
			}

			// 10/21/16 Igor TP#32770
			Benefit += PreFundAmount;

			// 09/30/19 Igor TP#41655 - consider debit card claw-backs
			//if (Group_blk.DebitRxPlan == 'H') // 12/06/21 Igor TP#4921 - have to consider all previous claims
			strcpy_s(szStmt, sizeof(szStmt), "SELECT SUM(DRT.Amount)"
				" FROM CLAIMS.DebitRxNonClaimEvents DRNCE WITH (NOLOCK)"
				" INNER JOIN CLAIMS.DebitRxTransactions DRT WITH (NOLOCK) ON DRNCE.DebitRxNonClaimEventID = DRT.ActivityID AND DRT.ActivityType = 'O'"
				" WHERE DRNCE.EventTypeID = 2 AND DRNCE.MActiveUniqueID IN"
				" (SELECT a.Unique_ID FROM CLAIMS.M_Active a WITH (NOLOCK)");

			if (Individual_Only)
			{
				strcat_s(szStmt, sizeof(szStmt), " WHERE a.Claimant_ID");

				if (Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS || Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS)
					sprintf_s(buff, sizeof(buff), " IN (SELECT Unique_ID FROM CLAIMS.M_Claimant WITH (NOLOCK)"
						" WHERE MasterClaimantID=%ld)", Claimant_blk.MasterClaimantID);
				else
					sprintf_s(buff, sizeof(buff), "=%ld", Claimant_blk.Unique_ID);

				strcat_s(szStmt, sizeof(szStmt), buff);
			}
			else //family
			{
				sprintf_s(buff, sizeof(buff), " INNER JOIN CLAIMS.M_Claimant c WITH (NOLOCK)"
					" ON a.Claimant_ID=c.Unique_ID"
					" WHERE c.Member_Number='%s' AND c.Plan_ID=a.Plan_ID",
					Claimant_blk.MemberNumber);

				strcat_s(szStmt, sizeof(szStmt), buff);
			}

			if (Group_blk.MaxBenefitIndividualByProduct)
			{
				sprintf_s(buff, sizeof(buff), " AND a.Service_ID IN (SELECT Service_ID FROM Claims.m_Product_Control"
					" WITH (NOLOCK)"
					" WHERE PlanProductID=%ld)", Group_blk.PlanProductID);

				strcat_s(szStmt, sizeof(szStmt), buff);

				if (Group_blk.CPL_Rule != LINKED_CLAIMANT_CARDS && Group_blk.CPL_Rule != MULTIPLE_CLAIMANT_CARDS)
				{
					sprintf_s(buff, sizeof(buff), " AND a.Plan_ID=%ld", Group_blk.Unique_ID);

					strcat_s(szStmt, sizeof(szStmt), buff);
				}
			}

			if (Group_blk.MaxBenefitIndividualRule != 9)
			{
				sprintf_s(buff, sizeof(buff), " AND (a.Date_Filled BETWEEN '%s' AND '%s')", BenefitStartDate, BenefitEndDate);
				strcat_s(szStmt, sizeof(szStmt), buff);
			}

			/*
			if (Group_blk.UseHistoricalData && Individual_Only)
			{
				sprintf_s(szStmt, sizeof(szStmt),"UNION ALL SELECT a.Unique_ID");

				sprintf_s(buff, sizeof(buff),")"
					" FROM CLAIMS.HistoricalPaidClaims a"
					" WITH (NOLOCK, INDEX (idx_HistPaid_InsLookback))" // 03/27/14 Igor TP#25182 // 11/06/18 Igor TP#39850  - added INDEX
					" INNER JOIN CLAIMS.HistoricalPaidClaimAmounts ca WITH (NOLOCK) ON ca.Activity_ID = a.Unique_ID" // 03/27/14 Igor TP#25182 - added WITH (NOLOCK)
					" WHERE (a.Date_Filled BETWEEN '%s' AND '%s') AND a.Claimant_ID",
					BenefitStartDate, BenefitEndDate);
				strcat_s(szStmt, sizeof(szStmt), buff);

				if (Group_blk.CPL_Rule == LINKED_CLAIMANT_CARDS // 02/06/14 Igor TP#24825 - added CPL_Rule 6 logic
					|| Group_blk.CPL_Rule == MULTIPLE_CLAIMANT_CARDS) // 02/08/16 Igor TP#31209 - added CPL_Rule 10 logic
					sprintf_s(buff, sizeof(buff)," IN (SELECT Unique_ID FROM CLAIMS.M_Claimant"
					" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
					" WHERE MasterClaimantID=%ld)", Claimant_blk.MasterClaimantID); // 02/24/14 Igor TP#25091 - using MasterClaimantID instead of Unique_ID
				else
					sprintf_s(buff, sizeof(buff),"=%ld", Claimant_blk.Unique_ID);
				strcat_s(szStmt, sizeof(szStmt), buff);
			)
			*/
			strcat_s(szStmt, sizeof(szStmt), ")");

			pStmt = gOdbcManager.GetStatement(true);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->Execute(szStmt);

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
			{
				double TotalClawbackAmount = pStmt->GetNumeric(0);

				if (TotalClawbackAmount > 0)
				{
					reporter.info("Considering unused clawback amount of <%.2f>", TotalClawbackAmount);
					Benefit -= TotalClawbackAmount;
				}
			}
		}
		else // MaxBenefitIndividualRule == 10 || 11
		{
			// 04/24/23 Troy OCLE-4856
			char LogFileOutput[500] { 0 };
			
			// 10/30/20 Troy TP#45555
			pStmt = gOdbcManager.GetStatement(true);

			strcpy_s(szStmt, sizeof(szStmt), "{CALL ");
			strcat_s(szStmt, sizeof(szStmt), Group_blk.CustomMaxBenefitIndividualSP); // M_Plan.CustomMaxBenefitIndividualSP - Naming convention usp_CE_MBIS_XXXXXX (f.e. CLAIMS.usp_CE_MBIS_)
			strcat_s(szStmt, sizeof(szStmt), "("
				"?," //@PlanID INT
				"?," //@ClaimantID INT,
				"?," //@MasterClaimantID INT,
				"?," //@DateFilled DATETIME
				"?"  //@NDC VARCHAR(11) - // 04/24/23 Troy OCLE-4856
				")}");

			pStmt->Prepare(szStmt);

			if (!pStmt->BindParamsAuto())
			{
				reporter.error("Query contains no parameters");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			pStmt->SetInt(Group_blk.Unique_ID);
			pStmt->SetInt(Claimant_blk.Unique_ID);
			pStmt->SetInt(Claimant_blk.MasterClaimantID);
			pStmt->SetDateString(pRX_Message->DateFilled);
			pStmt->SetCharPtr(pRX_Message->Claim.NDCNumber); // 04/24/23 Troy OCLE-4856

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->ExecutePrepared();

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			if (pStmt->First())
			{
				Benefit = pStmt->GetNumeric(0);
				pStmt->GetDatePackedString("BenefitStartDate", BenefitStartDate, sizeof(BenefitStartDate));
				pStmt->GetDatePackedString("BenefitEndDate", BenefitEndDate, sizeof(BenefitEndDate));

				// 04/24/23 Troy OCLE-4856
				pStmt->GetString("LogFileOutput", LogFileOutput, sizeof(LogFileOutput));
				if (LogFileOutput[0])
					reporter.info("Additional Details on Custom Accumulated Benefit: %s", LogFileOutput);
			}
		}

		// 11/23/20 Igor TP#45711 - store PatientBenefitStartDate and PatientBenefitEndDate in Claimant_blk
		strcpy_s(Claimant_blk.PatientBenefitStartDate, sizeof(Claimant_blk.PatientBenefitStartDate), BenefitStartDate);
		strcpy_s(Claimant_blk.PatientBenefitEndDate, sizeof(Claimant_blk.PatientBenefitEndDate), BenefitEndDate);
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return RETURN_SUCCESS;
}

double PriorFillsAverageQtyDispensed(long ClaimantID, int scripts_back)
{
	Reporter reporter(MonitorFile, "PriorFillsAverageQtyDispensed");

	char szStmt[1024];
	double total = 0;
	int rows = 0;

	OdbcStatement *pStmt = NULL;

	try
	{
		for (int tables = 1; tables <= 2 && rows != scripts_back; ++tables)
		{
			pStmt = gOdbcManager.GetStatement();

			sprintf_s(szStmt, sizeof(szStmt), "SELECT TOP %d Metric_Dec_Qty"
				" FROM %s"
				" WHERE CLAIMANT_ID=?"
				" ORDER BY UNIQUE_ID DESC",
				scripts_back, (tables == 1) ? "CLAIMS.M_Active WITH (NOLOCK, INDEX (InsLookbackIdx))" : "CLAIMS.HistoricalPaidClaims WITH (NOLOCK, INDEX (idx_HistPaid_InsLookback))"); // 11/06/18 Igor TP#39850  - added INDEX

			pStmt->Prepare(szStmt);

			if (!pStmt->BindParamsAuto())
			{
				reporter.error("Query contains no parameters");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			pStmt->SetInt(ClaimantID);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->ExecutePrepared();

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

			if (!pStmt->BindAuto())
			{
				reporter.error("Cursor doesn't contain resultset");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}

			for (bool bRC = pStmt->First(); bRC; bRC = pStmt->Next())
			{
				++rows;
				total += pStmt->GetNumeric("Metric_Dec_Qty");
			}

			scripts_back -= rows;

			gOdbcManager.ReleaseStatement();
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	if (rows)
		return(total / (double)rows);
	else
		return(0.0);
}

// 8/28/10 BSS MSOE-63
int GetPriorSecondaryClaims(char *LastDateFilled, size_t size)
{
	Reporter reporter(MonitorFile, "CheckForPriorSecondaryClaims");

	char szStmt[1024];
	int Count = 0;

	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		// 06/03/15 Igor TP#29345 - replaced SQL with stored procedure
		strcpy_s(szStmt, sizeof(szStmt), "{CALL CLAIMS.usp_CE_GetPriorSecondaryClaims("
			"?," //@InsuranceVerificationLookbackPeriod INT
			"?," //@FirstNameKey1 SMALLINT
			"?," //@FirstNameKey2 SMALLINT
			"?," //@LastNameKey1 SMALLINT
			"?," //@LastNameKey2 SMALLINT
			"?," //@DateOfBirth DATETIME
			"?," //@Sex_Code CHAR(1)
			"?"  //@ZipCode VARCHAR(15)
			")}");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(Group_blk.InsuranceVerificationLookbackPeriod * -1);
		pStmt->SetInt(Claimant_blk.firstname_key_1);
		pStmt->SetInt(Claimant_blk.firstname_key_2);
		pStmt->SetInt(Claimant_blk.lastname_key_1);
		pStmt->SetInt(Claimant_blk.lastname_key_2);
		pStmt->SetDateString(Claimant_blk.DateOfBirth);
		pStmt->SetChar(Claimant_blk.SexCode);
		pStmt->SetCharPtr(Claimant_blk.ZipCode);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (pStmt->First())
		{
			Count = pStmt->GetInt("cnt");

			if (Count && size) // 04/21/16 Igor - fixed misspelled &
				pStmt->GetDatePackedString("Maxdate", LastDateFilled, size);

			if (pStmt->Next())
			{
				reporter.error("Returned more than 1 record");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}
		}
		else
		{
			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return Count;
}

// 10/26/12 Igor TP#22610
int GetPlanOtherPayerRules(Group_Info &blk)
{
	Reporter reporter(MonitorFile, "GetPlanOtherPayerRules");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		strcpy_s(szStmt, sizeof(szStmt), "SELECT OtherPayerCoverageType,OtherPayerIDQualifier,OtherPayerID,GroupNumber,IsRequired"
			" FROM CLAIMS.PlanOtherPayerRules"
			" WITH (NOLOCK)" // 03/25/14 Igor TP#25182
			" WHERE PlanID=?");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(blk.Unique_ID);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		for (bool bRC = pStmt->First(); bRC; bRC = pStmt->Next())
		{
			OtherPayerRules* otherPayerRules = new OtherPayerRules();
			pStmt->GetString("OtherPayerCoverageType", otherPayerRules->PrimaryPayerCoverageType, sizeof(otherPayerRules->PrimaryPayerCoverageType));
			pStmt->GetString("OtherPayerIDQualifier", otherPayerRules->PrimaryPayerIDQualifier, sizeof(otherPayerRules->PrimaryPayerIDQualifier));
			pStmt->GetString("OtherPayerID", otherPayerRules->PrimaryPayerID, sizeof(otherPayerRules->PrimaryPayerID));
			pStmt->GetString("GroupNumber", otherPayerRules->GroupNumber, sizeof(otherPayerRules->GroupNumber));
			otherPayerRules->IsRequired = pStmt->GetInt("IsRequired");
			blk.OtherPayerRulesArray.push_back(otherPayerRules);
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return RETURN_FAILURE;
}


int RecordClaimAmounts(ClaimAmount_Info &Amount_blk, RX_Message_struct* pRX_Message)//12/23/11 Igor - TP#16241 Added RX_Message_struct and slot
{
	Reporter reporter(MonitorFile, "RecordClaimAmounts");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	long ClaimAmountID;

	try
	{
		// 08/14/14 Igor TP#26154 - replaced SQL with stored procedure
		// insert record into CLAIMS.M_ClaimAmounts
		strcpy_s(szStmt, sizeof(szStmt), "{? = CALL CLAIMS.usp_CE_InsertClaimAmounts("
			"?," // @Account_ID INT
			"?," // @Plan_ID INT = NULL
			"?," // @Activity_ID INT = NULL
			"?," // @Transaction_Type SMALLINT = NULL
			"?," // @Cost NUMERIC(9, 4) = NULL
			"?," // @Quantity NUMERIC(8, 3) = NULL
			"?," // @SalesTax NUMERIC(7, 2) = NULL
			"?," // @ListPrice NUMERIC(7, 2) = NULL
			"?," // @Percentage NUMERIC(5, 4) = NULL
			"?," // @TwoTierPercentage NUMERIC(5, 4) = NULL
			"?," // @TotalAmount NUMERIC(7, 2) = NULL
			"?," // @Deductible NUMERIC(10,2) = NULL,		-- 06/25/19 Igor TP#41305 - increased size
			"?," // @AdminFee NUMERIC(7, 2) = NULL
			"?," // @FeeForService NUMERIC(7, 2) = NULL
			"?," // @ProviderReimb NUMERIC(7, 2) = NULL
			"?," // @MemberPays NUMERIC(7, 2) = NULL
			"?," // @Submission_Date DATETIME = NULL
			"?," // @ProcessorFee NUMERIC(7, 2) = NULL
			"?," // @MailOrder NUMERIC(7, 2) = NULL
			"?," // @Elec_Fee NUMERIC(7, 2) = NULL
			"?," // @PlanProFee NUMERIC(5, 2) = NULL
			"?," // @CouponAmt NUMERIC(7, 2) = NULL
			"?," // @BasisOfReimbursement INT = NULL
			"?," // @OpusFee NUMERIC(5, 2) = NULL
			"?," // @SponsorFee NUMERIC(5, 2) = NULL
			"?," // @BenefitDeterminationMethod INT = NULL
			"?," // @PaymentMethod CHAR(1) = NULL
			"?," // @BenefitRemaining NUMERIC(7, 2) = NULL
			"?," // @DebitCardLoadAmount NUMERIC(7, 2) = NULL
			"?," // @ProviderDirectPayBenefitAmount NUMERIC(7, 2) = NULL
			"?"  // @NumExpandedBenefits FLOAT = NULL     -- 02/27/23 Igor OCLE-4729
			")}");

		pStmt->Prepare(szStmt);

		// 08/14/14 Igor TP#26154 - assign the return code to ClaimAmountID variable address
		pStmt->SetReturnInt(&ClaimAmountID);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(Amount_blk.Account_ID);
		pStmt->SetInt(Amount_blk.Plan_ID);
		pStmt->SetInt(Amount_blk.Activity_ID);
		pStmt->SetInt(Amount_blk.TransactionType);
		pStmt->SetNumeric(Amount_blk.Cost);
		pStmt->SetNumeric(Amount_blk.Quantity);
		pStmt->SetNumeric(Amount_blk.SalesTax);
		pStmt->SetNumeric(Amount_blk.ListPrice);
		pStmt->SetNumeric(Amount_blk.Percentage);
		pStmt->SetNumeric(Amount_blk.TwoTierPercentage);
		pStmt->SetNumeric(Amount_blk.TotalAmount);
		pStmt->SetNumeric(Amount_blk.Deductible);
		pStmt->SetNumeric(Amount_blk.AdminFee);
		pStmt->SetNumeric(Amount_blk.PharmacyFee);
		pStmt->SetNumeric(Amount_blk.ProviderReimb);
		pStmt->SetNumeric(Amount_blk.MemberPays);
		pStmt->SetDateString(Amount_blk.SubmissionDate);
		pStmt->SetNumeric(Amount_blk.ProcessorFee);
		pStmt->SetNumeric(Amount_blk.MailOrder);
		pStmt->SetNumeric(Amount_blk.Elec_Fee);
		pStmt->SetNumeric(Amount_blk.PlanProFee);
		pStmt->SetNumeric(Amount_blk.CouponAmount);
		pStmt->SetNumeric(Amount_blk.BasisOfReimbursement);
		pStmt->SetNumeric(Amount_blk.OpusFee);
		pStmt->SetNumeric(Amount_blk.SponsorFee);
		pStmt->SetLongLong(Amount_blk.BenefitDeterminationMethod);	// 01/21/14 Igor TP#24803 // 02/11/22 Igor TP#49513
		pStmt->SetChar(Amount_blk.PaymentMethod);				// 10/31/14 Igor TP#27165
		if (Amount_blk.BenefitRemaining == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.BenefitRemaining); //12/10/15 Igor TP#30675
		pStmt->SetNumeric(Amount_blk.DebitCardLoadAmount); // 10/13/16 Igor TP#32771
		pStmt->SetNumeric(Amount_blk.ProviderDirectPayBenefitAmount); // 03/13/18 Igor TP#37811
		pStmt->SetNumeric(pRX_Message->NumExpandedBenefitsApplied); // 02/27/23 Igor OCLE-4729

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold, ClaimAmountID);

		if (!ClaimAmountID)
		{
			reporter.debug("Invalid return value");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt = gOdbcManager.GetStatement(true);

		char szQuery[100];
		int bFirst = 1;
		char maxOtherPayerCoverageType[3];
		maxOtherPayerCoverageType[0] = 0;
		GetTopOtherPayerCoverageType(pRX_Message, maxOtherPayerCoverageType, sizeof(maxOtherPayerCoverageType));

		strcpy_s(szStmt, sizeof(szStmt), "UPDATE CLAIMS.M_ClaimAmounts SET ");

		// 01/16/12 Igor - replaced iterator with operator[]
		for (unsigned int i = 0; i < pRX_Message->Claim.COBArray.size(); i++)
		{
			if (!strcmp(pRX_Message->Claim.COBArray[i]->PrimaryPayerCoverageType, maxOtherPayerCoverageType))
			{
				for (std::vector<PrimaryPayer*>::iterator iterOtherPayer = pRX_Message->Claim.COBArray[i]->PrimaryPayerArray.begin();
					iterOtherPayer != pRX_Message->Claim.COBArray[i]->PrimaryPayerArray.end(); iterOtherPayer++)
				{
					if ((*iterOtherPayer)->AmountPayable > 0)
					{
						if (!bFirst)
							strcat_s(szStmt, sizeof(szStmt), ",");

						if (strlen((*iterOtherPayer)->DatabaseField) > 0) // 12/29/11 Igor
						{
							sprintf_s(szQuery, sizeof(szQuery), "%s = %g", (*iterOtherPayer)->DatabaseField, (*iterOtherPayer)->AmountPayable);
							strcat_s(szStmt, sizeof(szStmt), szQuery);
							bFirst = 0;
						}
					}
				}
			}
		}

		if (!bFirst)
		{
			sprintf_s(szQuery, sizeof(szQuery), " WHERE Unique_ID = %ld", ClaimAmountID);
			strcat_s(szStmt, sizeof(szStmt), szQuery);

			reporter.timedStart(pStmt->PrintSql(szStmt));

			pStmt->Execute(szStmt);

			reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return RETURN_SUCCESS;
}

long SaveVoidAmounts(ClaimAmount_Info &Amount_blk, DebitRxTransaction &DebitRxTransaction_blk) // 04/23/19 Igor TP#40908 - added DebitRxTransaction parameter
{
	Reporter reporter(MonitorFile, "SaveVoidAmounts");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	long VoidAmountsID = 0;

	try
	{
		// 12/16/19 Igor TP#42048 - converted SQL to stored procedure
		strcpy_s(szStmt, sizeof(szStmt), "{? = CALL CLAIMS.usp_CE_InsertVoidAmounts(" // insert record into CLAIMS.M_VoidAmounts
			"?," // @Account_ID INT
			"?," // @Plan_ID INT = NULL
			"?," // @Activity_ID INT = NULL
			"?," // @Transaction_Type SMALLINT = NULL
			"?," // @Cost NUMERIC(9, 4) = NULL
			"?," // @Quantity NUMERIC(8, 3) = NULL
			"?," // @SalesTax NUMERIC(7, 2) = NULL
			"?," // @ListPrice NUMERIC(7, 2) = NULL
			"?," // @Percentage NUMERIC(5, 4) = NULL
			"?," // @TwoTierPercentage NUMERIC(5, 4) = NULL
			"?," // @TotalAmount NUMERIC(7, 2) = NULL
			"?," // @Deductible NUMERIC(10,2) = NULL,		
			"?," // @AdminFee NUMERIC(7, 2) = NULL
			"?," // @FeeForService NUMERIC(7, 2) = NULL
			"?," // @ProviderReimb NUMERIC(7, 2) = NULL
			"?," // @MemberPays NUMERIC(7, 2) = NULL
			"?," // @Submission_Date DATETIME = NULL
			"?," // @ProcessorFee NUMERIC(7, 2) = NULL
			"?," // @MailOrder NUMERIC(7, 2) = NULL
			"?," // @Elec_Fee NUMERIC(7, 2) = NULL
			"?," // @PlanProFee NUMERIC(5, 2) = NULL
			"?," // @CouponAmt NUMERIC(7, 2) = NULL
			"?," // @BasisOfReimbursement INT = NULL
			"?," // @OpusFee NUMERIC(5, 2) = NULL
			"?," // @SponsorFee NUMERIC(5, 2) = NULL
			"?," // @BenefitDeterminationMethod BIGINT = NULL
			"?," // @PaymentMethod CHAR(1) = NULL
			"?," // @AmtAppliedToDeductible NUMERIC(7, 2) = NULL
			"?," // @AmtForBrandDrug NUMERIC(7, 2) = NULL
			"?," // @AmtForSalesTax NUMERIC(7, 2) = NULL
			"?," // @AmtExceedingPeriodicBenefitMax NUMERIC(7, 2) = NULL
			"?," // @AmtForCopay NUMERIC(7, 2) = NULL
			"?," // @AmtForCoinsurance NUMERIC(7, 2) = NULL
			"?," // @AmtForNonPreferredFormularySelection NUMERIC(7, 2) = NULL
			"?," // @AmtForHealthPlanAssistance NUMERIC(7, 2) = NULL
			"?," // @AmtForProviderNetworkSolution NUMERIC(7, 2) = NULL
			"?," // @AmtForBrandNonPreferredFormularySelection NUMERIC(7, 2) = NULL
			"?," // @AmtForCoverageGap NUMERIC(7, 2) = NULL
			"?," // @AmtForProcessorFee NUMERIC(7, 2) = NULL
			"?," // @BenefitRemaining NUMERIC(10, 2) = NULL
			"?," // @DebitCardLoadAmount NUMERIC(10, 2) = NULL
			"?," // @DebitCardLoadAmount_M_Active NUMERIC(10, 2) = NULL
			"?," // @ProviderDirectPayBenefitAmount NUMERIC(10, 2) = NULL
			"?"  // @DebitRxStatus INT = NULL
			")}");

		pStmt->Prepare(szStmt);

		// 08/14/14 Igor TP#26154 - assign the return code to ClaimAmountID variable address
		pStmt->SetReturnInt(&VoidAmountsID);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(Amount_blk.Account_ID);
		pStmt->SetInt(Amount_blk.Plan_ID);
		pStmt->SetInt(Amount_blk.Activity_ID);
		pStmt->SetInt(Amount_blk.TransactionType);
		pStmt->SetNumeric(Amount_blk.Cost);
		pStmt->SetNumeric(Amount_blk.Quantity);
		pStmt->SetNumeric(Amount_blk.SalesTax);
		pStmt->SetNumeric(Amount_blk.ListPrice);
		pStmt->SetNumeric(Amount_blk.Percentage);
		pStmt->SetNumeric(Amount_blk.TwoTierPercentage);
		pStmt->SetNumeric(Amount_blk.TotalAmount);
		pStmt->SetNumeric(Amount_blk.Deductible);
		pStmt->SetNumeric(Amount_blk.AdminFee);
		pStmt->SetNumeric(Amount_blk.PharmacyFee);
		pStmt->SetNumeric(Amount_blk.ProviderReimb);
		pStmt->SetNumeric(Amount_blk.MemberPays);
		pStmt->SetDateString(Amount_blk.SubmissionDate);
		pStmt->SetNumeric(Amount_blk.ProcessorFee);
		pStmt->SetNumeric(Amount_blk.MailOrder);
		pStmt->SetNumeric(Amount_blk.Elec_Fee);
		pStmt->SetNumeric(Amount_blk.PlanProFee);
		pStmt->SetNumeric(Amount_blk.CouponAmount);
		pStmt->SetNumeric(Amount_blk.BasisOfReimbursement);
		pStmt->SetNumeric(Amount_blk.OpusFee);
		pStmt->SetNumeric(Amount_blk.SponsorFee);
		pStmt->SetLongLong(Amount_blk.BenefitDeterminationMethod);	// 01/21/14 Igor TP#24803 // 02/11/22 Igor TP#49513
		pStmt->SetChar(Amount_blk.PaymentMethod);				// 10/31/14 Igor TP#27165
		// 03/30/15 Igor TP#28661 - added 12 COB OPPRA fields
		if (Amount_blk.AmtAppliedToDeductible == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtAppliedToDeductible);
		if (Amount_blk.AmtForBrandDrug == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForBrandDrug);
		if (Amount_blk.AmtForSalesTax == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForSalesTax);
		if (Amount_blk.AmtExceedingPeriodicBenefitMax == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtExceedingPeriodicBenefitMax);
		if (Amount_blk.AmtForCopay == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForCopay);
		if (Amount_blk.AmtForCoinsurance == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForCoinsurance);
		if (Amount_blk.AmtForNonPreferredFormularySelection == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForNonPreferredFormularySelection);
		if (Amount_blk.AmtForHealthPlanAssistance == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForHealthPlanAssistance);
		if (Amount_blk.AmtForProviderNetworkSolution == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForProviderNetworkSolution);
		if (Amount_blk.AmtForBrandNonPreferredFormularySelection == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForBrandNonPreferredFormularySelection);
		if (Amount_blk.AmtForCoverageGap == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForCoverageGap);
		if (Amount_blk.AmtForProcessorFee == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.AmtForProcessorFee);
		if (Amount_blk.BenefitRemaining == -1)
			pStmt->SetNull();
		else
			pStmt->SetNumeric(Amount_blk.BenefitRemaining);				// 12/10/15 Igor TP#30675
		pStmt->SetNumeric(DebitRxTransaction_blk.Amount);				// 10/13/16 Igor TP#32771 // 04/23/19 Igor TP#40908 - using Amount from DebitRxTransaction_blk
		pStmt->SetNumeric(Amount_blk.DebitCardLoadAmount);				// 04/23/19 Igor TP#40908 - set DebitCardLoadAmount_M_Active field
		pStmt->SetNumeric(Amount_blk.ProviderDirectPayBenefitAmount);	// 03/13/18 Igor TP#37811
		pStmt->SetInt(Amount_blk.DebitRxStatus);						// 12/16/19 Igor TP#42048

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return VoidAmountsID;
}

long DeleteClaimAmounts(long ActivityID)
{
	Reporter reporter(MonitorFile, "DeleteClaimAmounts");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	long deletedRows = 0;

	try
	{
		// DELETE FROM CLAIMS.M_ClaimAmounts
		strcpy_s(szStmt, sizeof(szStmt), "{? = CALL CLAIMS.usp_CE_DeleteClaimAmounts(" // 04/03/20 Igor TP#43702 - converted to sp
			"?" // @Activity_ID INT = NULL
			")}");

		pStmt->Prepare(szStmt);

		pStmt->SetReturnInt(&deletedRows);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(ActivityID);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return deletedRows;
}

// ID	- link between Active record and Claim Amounts record
// Amount - structure containing claim amount information
// Returns: 1 if found, 0 if not found, SQL error status if error
long GetClaimAmountsRecord(long ID, ClaimAmount_Info &Amount_blk)
{
	Reporter reporter(MonitorFile, "GetClaimAmountsRecord");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		// 12/17/19 Igor - TP#42048 replaced SQL with stored procedure
		strcpy_s(szStmt, sizeof(szStmt), "{CALL CLAIMS.usp_CE_GetClaimAmounts" // get values from CLAIMS.M_ClaimAmounts
			"(@Activity_ID=?)}");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(ID);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (pStmt->First())
		{
			Amount_blk.Activity_ID = ID;
			Amount_blk.Account_ID = pStmt->GetLong("Account_ID");
			Amount_blk.Plan_ID = pStmt->GetLong("Plan_ID");
			Amount_blk.TransactionType = pStmt->GetInt("Transaction_Type");
			Amount_blk.Cost = pStmt->GetNumeric("Cost");
			Amount_blk.Quantity = pStmt->GetNumeric("Quantity");
			Amount_blk.SalesTax = pStmt->GetNumeric("SalesTax");
			Amount_blk.ListPrice = pStmt->GetNumeric("ListPrice");
			Amount_blk.Percentage = pStmt->GetNumeric("Percentage");
			Amount_blk.TwoTierPercentage = pStmt->GetNumeric("TwoTierPercentage");
			Amount_blk.TotalAmount = pStmt->GetNumeric("TotalAmount");
			Amount_blk.Deductible = pStmt->GetNumeric("Deductible");
			Amount_blk.AdminFee = pStmt->GetNumeric("AdminFee");
			Amount_blk.PharmacyFee = pStmt->GetNumeric("FeeForService");
			Amount_blk.ProviderReimb = pStmt->GetNumeric("ProviderReimb");
			Amount_blk.MemberPays = pStmt->GetNumeric("MemberPays");
			pStmt->GetDatePackedString("Submission_Date", Amount_blk.SubmissionDate, sizeof(Amount_blk.SubmissionDate));
			Amount_blk.ProcessorFee = pStmt->GetNumeric("ProcessorFee");
			Amount_blk.MailOrder = pStmt->GetNumeric("MailOrder");
			Amount_blk.Elec_Fee = pStmt->GetNumeric("Elec_Fee");
			Amount_blk.PlanProFee = pStmt->GetNumeric("PlanProFee");
			Amount_blk.CouponAmount = pStmt->GetNumeric("CouponAmt");
			Amount_blk.BasisOfReimbursement = pStmt->GetInt("BasisOfReimbursement"); // 9/16/10 BSS MSOE-20
			// 05/30/12 Igor TP#19646 - added OpusFee and SponsorFee
			Amount_blk.OpusFee = pStmt->GetNumeric("OpusFee");
			Amount_blk.SponsorFee = pStmt->GetNumeric("SponsorFee");
			Amount_blk.BenefitDeterminationMethod = pStmt->GetLongLong("BenefitDeterminationMethod"); // 01/21/14 Igor TP#24803 // 02/11/22 Igor TP#49513
			pStmt->GetChar("PaymentMethod", &Amount_blk.PaymentMethod);							 // 10/31/14 Igor TP#27165
			// 03/30/15 Igor TP#28661 - added 12 COB OPPRA fields
			Amount_blk.AmtAppliedToDeductible = pStmt->GetNumeric("AmtAppliedToDeductible", -1);
			Amount_blk.AmtForBrandDrug = pStmt->GetNumeric("AmtForBrandDrug", -1);
			Amount_blk.AmtForSalesTax = pStmt->GetNumeric("AmtForSalesTax", -1);
			Amount_blk.AmtExceedingPeriodicBenefitMax = pStmt->GetNumeric("AmtExceedingPeriodicBenefitMax", -1);
			Amount_blk.AmtForCopay = pStmt->GetNumeric("AmtForCopay", -1);
			Amount_blk.AmtForCoinsurance = pStmt->GetNumeric("AmtForCoinsurance", -1);
			Amount_blk.AmtForNonPreferredFormularySelection = pStmt->GetNumeric("AmtForNonPreferredFormularySelection", -1);
			Amount_blk.AmtForHealthPlanAssistance = pStmt->GetNumeric("AmtForHealthPlanAssistance", -1);
			Amount_blk.AmtForProviderNetworkSolution = pStmt->GetNumeric("AmtForProviderNetworkSolution", -1);
			Amount_blk.AmtForBrandNonPreferredFormularySelection = pStmt->GetNumeric("AmtForBrandNonPreferredFormularySelection", -1);
			Amount_blk.AmtForCoverageGap = pStmt->GetNumeric("AmtForCoverageGap", -1);
			Amount_blk.AmtForProcessorFee = pStmt->GetNumeric("AmtForProcessorFee", -1);
			Amount_blk.DebitCardLoadAmount = pStmt->GetNumeric("DebitCardLoadAmount"); // 10/25/16 Igor TP#33037
			Amount_blk.ProviderDirectPayBenefitAmount = pStmt->GetNumeric("ProviderDirectPayBenefitAmount"); // 03/13/18 Igor TP#37811

			if (pStmt->Next())
			{
				reporter.error("Returned more than 1 record");
				gOdbcManager.ReleaseStatement(true);
				return SQL_PROCESSING_ERROR;
			}
		}
		else
		{
			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return RETURN_SUCCESS;
}

//Determines if any records exists in M_PriorApproval table for this plan
long PriorApprovalGPI(long Plan_ID, char *GPI_Code)
{
	Reporter reporter(MonitorFile, "PriorApprovalGPI");

	char szStmt[1024];
	int UniqueID = 0;

	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		char gpi[10];
		// tgpi has six digit gpi code ie 941000  class, subclass 1, subclass 2
		// ie gpi has eight digit code, 00 appended ie 94100000,  Drug Name =  all
		// GPI_Code is the original eight digit gpi code ie 94102046
		memcpy(gpi, GPI_Code, 6);
		memcpy(gpi + 6, "00", 2);
		gpi[8] = '%';
		gpi[9] = 0;

		strcpy_s(szStmt, sizeof(szStmt), "SELECT Unique_ID,GPI_Code"
			" FROM CLAIMS.M_PriorApproval"
			" WITH (NOLOCK)" // 03/25/14 Igor TP#25182
			" WHERE Plan_ID=? AND PA_GPI LIKE ?");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(Plan_ID);
		pStmt->SetCharPtr(GPI_Code);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		const char *PA_GPI;

		for (bool bRC = pStmt->First(); bRC; bRC = pStmt->Next())
		{
			UniqueID = pStmt->GetLong("Unique_ID");
			PA_GPI = pStmt->GetCharPtr("PA_GPI");

			// test it GPI_Code matches the GPI found
			if (memcmp(gpi, PA_GPI, 8) == 0)
				break;
			else if (memcmp(GPI_Code, PA_GPI, 8) == 0)
				break;
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return UniqueID;
}

int Claimant_Record_Exists(Claimant_Info &blk)
{
	Reporter reporter(MonitorFile, "Claimant_Record_Exists");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		strcpy_s(szStmt, sizeof(szStmt), "SELECT TOP 1 Claimant_ID"
			" FROM CLAIMS.M_Active"
			" WITH (NOLOCK)" // 08/04/15 Igor TP#29763 Added WITH (NOLOCK)
			" WHERE Claimant_ID=?");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(blk.Unique_ID);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (!pStmt->First())
		{
			gOdbcManager.ReleaseStatement();
			return RETURN_FAILURE;
		}
	}
	catch (OdbcException *pE)
	{
		reporter.error(pE->getMsg());
		gOdbcManager.ReleaseStatement(true);
		delete pE;
		return SQL_PROCESSING_ERROR;
	}

	gOdbcManager.ReleaseStatement();
	return RETURN_SUCCESS;
}

//04/19/12 Igor - changed input parameter to DateFilled
int ApplyHistoryRules(char* DateFilled, Group_Info &blk, BOOL isReversal)
{
	Reporter reporter(MonitorFile, "ApplyHistoryRules");

	char szStmt[1024];
	OdbcStatement *pStmt = gOdbcManager.GetStatement();

	try
	{
		// 08/11/14 Igor - replaced SQL with stored procedure
		strcpy_s(szStmt, sizeof(szStmt), "{CALL CLAIMS.usp_CE_GetPlanHistory" // using CLAIMS.PlanHistory table
			"(@PlanID=?"
			",@RuleDate=?)}");

		pStmt->Prepare(szStmt);

		if (!pStmt->BindParamsAuto())
		{
			reporter.error("Query contains no parameters");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		pStmt->SetInt(blk.Unique_ID);
		pStmt->SetDateString(DateFilled);

		reporter.timedStart(pStmt->PrintSql(szStmt));

		pStmt->ExecutePrepared();

		reporter.timedEnd(System_Settings.ExcessiveProcessingThreshold);

		if (!pStmt->BindAuto())
		{
			reporter.error("Cursor doesn't contain resultset");
			gOdbcManager.ReleaseStatement(true);
			return SQL_PROCESSING_ERROR;
		}

		if (pStmt->First())
		{
			char StartDate[25], EndDate[25];
			const OdbcStatement::DATE *date = pStmt->GetDate("RuleStartDate");
			sprintf_s(StartDate, sizeof(StartDate), "%4.4d-%2.2d-%2.2d", date->iYear, date->iMonth, date->iDay);//CCYY-MM-DD
			date = pStmt->GetDate("RuleEndDate");
			sprintf_s(EndDate, sizeof(EndDate), "%4.4d-%2.2d-%2.2d", date->iYear, date->iMonth, date->iDay);//CCYY-MM-DD
			reporter.info("Loading new Benefit limits from PlanHistory (rule date between %s and %s)...", StartDate, EndDate);

			blk.Percent_For_Third_Party = pStmt->GetInt("Percent_For_Third_Party", blk.Percent_For_Third_Party);
			blk.Percent_For_Cash = pStmt->GetInt("Percent_For_Cash", blk.Percent_For_Cash);
			pStmt->GetString("OtherCoverageCodes", blk.OtherCoverageCodes, sizeof(blk.OtherCoverageCodes), blk.OtherCoverageCodes);
			blk.MinPatPayAmtCash = pStmt->GetNumeric("MinPatPayAmtCash", blk.MinPatPayAmtCash);
			blk.MinPatPayAmtInsured = pStmt->GetNumeric("MinPatPayAmtInsured", blk.MinPatPayAmtInsured);
			blk.MailMinPatPayAmtCash = pStmt->GetNumeric("MailMinPatPayAmtCash", blk.MailMinPatPayAmtCash);
			blk.MailMinPatPayAmtInsured = pStmt->GetNumeric("MailMinPatPayAmtInsured", blk.MailMinPatPayAmtInsured);
			blk.MaximumBenefitCash = pStmt->GetNumeric("MaximumBenefitCash", blk.MaximumBenefitCash);
			blk.MaximumBenefitInsured = pStmt->GetNumeric("MaximumBenefitInsured", blk.MaximumBenefitInsured);
			blk.MailMaximumBenefitCash = pStmt->GetNumeric("MailMaximumBenefitCash", blk.MailMaximumBenefitCash);
			blk.MailMaximumBenefitInsured = pStmt->GetNumeric("MailMaximumBenefitInsured", blk.MailMaximumBenefitInsured);
			blk.Specialty_Brand_AWP_Percentage = pStmt->GetNumeric("Specialty_Brand_AWP_Percentage", blk.Specialty_Brand_AWP_Percentage);
			blk.Specialty_Brand_Fee = pStmt->GetNumeric("Specialty_Brand_Fee", blk.Specialty_Brand_Fee);
			blk.Specialty_Single_Generic_AWP_Percentage = pStmt->GetNumeric("Specialty_Single_Generic_AWP_Percentage", blk.Specialty_Single_Generic_AWP_Percentage);
			blk.Specialty_Multi_Generic_AWP_Percentage = pStmt->GetNumeric("Specialty_Multi_Generic_AWP_Percentage", blk.Specialty_Multi_Generic_AWP_Percentage);
			blk.Specialty_Single_Generic_Fee = pStmt->GetNumeric("Specialty_Single_Generic_Fee", blk.Specialty_Single_Generic_Fee);
			blk.Specialty_Multi_Generic_Fee = pStmt->GetNumeric("Specialty_Multi_Generic_Fee", blk.Specialty_Multi_Generic_Fee);
			blk.Specialty_Maximum_Days_Supply = pStmt->GetInt("Specialty_Maximum_Days_Supply", blk.Specialty_Maximum_Days_Supply);
			blk.Specialty_Maximum_Metric_Qty = pStmt->GetNumeric("Specialty_Maximum_Metric_Qty", blk.Specialty_Maximum_Metric_Qty);
			blk.SpecialtyMaximumBenefitCash = pStmt->GetNumeric("SpecialtyMaximumBenefitCash", blk.SpecialtyMaximumBenefitCash);
			blk.SpecialtyMaximumBenefitInsured = pStmt->GetNumeric("SpecialtyMaximumBenefitInsured", blk.SpecialtyMaximumBenefitInsured);
			blk.SpecialtyMaximumRefills = pStmt->GetInt("SpecialtyMaximumRefills", blk.SpecialtyMaximumRefills);
			blk.SpecialtyMinPatPayAmtCash = pStmt->GetNumeric("SpecialtyMinPatPayAmtCash", blk.SpecialtyMinPatPayAmtCash);
			blk.SpecialtyMinPatPayAmtInsured = pStmt->GetNumeric("SpecialtyMinPatPayAmtInsured", blk.SpecialtyMinPatPayAmtInsured);
			blk.SpecialtyMultiBenefitRule = pStmt->GetInt("SpecialtyMultiBenefitRule", blk.SpecialtyMultiBenefitRule);
			// 01/06/15 Troy TP#28029
			//blk.SpecialtyNextFillAllowedAfter = pStmt->GetInt("SpecialtyNextFillAllowedAfter", blk.SpecialtyNextFillAllowedAfter);
			pStmt->GetString("SpecialtyRxDenialOverridesAllowed", blk.SpecialtyRxDenialOverridesAllowed, sizeof(blk.SpecialtyRxDenialOverridesAllowed), blk.SpecialtyRxDenialOverridesAllowed);
			blk.SpecialtyMinimumDaysSupply = pStmt->GetInt("SpecialtyMinimumDaysSupply", blk.SpecialtyMinimumDaysSupply);
			blk.SpecialtyMinimumMetricQty = pStmt->GetNumeric("SpecialtyMinimumMetricQty", blk.SpecialtyMinimumMetricQty);
			blk.Specialty_Brand_WAC_Percentage = pStmt->GetNumeric("Specialty_Brand_WAC_Percentage", blk.Specialty_Brand_WAC_Percentage);
			blk.Specialty_Single_Generic_WAC_Percentage = pStmt->GetNumeric("Specialty_Single_Generic_WAC_Percentage", blk.Specialty_Single_Generic_WAC_Percentage);
			blk.Specialty_Multi_Generic_WAC_Percentage = pStmt->GetNumeric("Specialty_Multi_Generic_WAC_Percentage", blk.Specialty_Multi_Generic_WAC_Percentage);
			blk.Ind_Brand_Cost_Basis = pStmt->GetInt("Ind_Brand_Cost_Basis", blk.Ind_Brand_Cost_Basis);
			blk.Ind_Single_Generic_Cost_Basis = pStmt->GetInt("Ind_Single_Generic_Cost_Basis", blk.Ind_Single_Generic_Cost_Basis);
			blk.Ind_Multi_Generic_Cost_Basis = pStmt->GetInt("Ind_Multi_Generic_Cost_Basis", blk.Ind_Multi_Generic_Cost_Basis);
			blk.Chn_Brand_Cost_Basis = pStmt->GetInt("Chn_Brand_Cost_Basis", blk.Chn_Brand_Cost_Basis);
			blk.Chn_Single_Generic_Cost_Basis = pStmt->GetInt("Chn_Single_Generic_Cost_Basis", blk.Chn_Single_Generic_Cost_Basis);
			blk.Chn_Multi_Generic_Cost_Basis = pStmt->GetInt("Chn_Multi_Generic_Cost_Basis", blk.Chn_Multi_Generic_Cost_Basis);
			blk.Mail_Brand_Cost_Basis = pStmt->GetInt("Mail_Brand_Cost_Basis", blk.Mail_Brand_Cost_Basis);
			blk.Mail_Single_Generic_Cost_Basis = pStmt->GetInt("Mail_Single_Generic_Cost_Basis", blk.Mail_Single_Generic_Cost_Basis);
			blk.Mail_Multi_Generic_Cost_Basis = pStmt->GetInt("Mail_Multi_Generic_Cost_Basis", blk.Mail_Multi_Generic_Cost_Basis);
			blk.Specialty_Brand_Cost_Basis = pStmt->GetInt("Specialty_Brand_Cost_Basis", blk.Specialty_Brand_Cost_Basis);
			blk.Specialty_Single_Generic_Cost_Basis = pStmt->GetInt("Specialty_Single_Generic_Cost_Basis", blk.Specialty_Single_Generic_Cost_Basis);
			blk.Specialty_Multi_Generic_Cost_Basis = pStmt->GetInt("Specialty_Multi_Generic_Cost_Basis", blk.Specialty_Multi_Generic_Cost_Basis);
			blk.Ind_Brand_Reimbursement_Formula = pStmt->GetInt("Ind_Brand_Reimbursement_Formula", blk.Ind_Brand_Reimbursement_Formula);
			blk.Ind_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Ind_Single_Generic_Reimbursement_Formula", blk.Ind_Single_Generic_Reimbursement_Formula);
			blk.Ind_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Ind_Multi_Generic_Reimbursement_Formula", blk.Ind_Multi_Generic_Reimbursement_Formula);
			blk.Chn_Brand_Reimbursement_Formula = pStmt->GetInt("Chn_Brand_Reimbursement_Formula", blk.Chn_Brand_Reimbursement_Formula);
			blk.Chn_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Chn_Single_Generic_Reimbursement_Formula", blk.Chn_Single_Generic_Reimbursement_Formula);
			blk.Chn_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Chn_Multi_Generic_Reimbursement_Formula", blk.Chn_Multi_Generic_Reimbursement_Formula);
			blk.Mail_Brand_Reimbursement_Formula = pStmt->GetInt("Mail_Brand_Reimbursement_Formula", blk.Mail_Brand_Reimbursement_Formula);
			blk.Mail_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Mail_Single_Generic_Reimbursement_Formula", blk.Mail_Single_Generic_Reimbursement_Formula);
			blk.Mail_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Mail_Multi_Generic_Reimbursement_Formula", blk.Mail_Multi_Generic_Reimbursement_Formula);
			blk.Specialty_Brand_Reimbursement_Formula = pStmt->GetInt("Specialty_Brand_Reimbursement_Formula", blk.Specialty_Brand_Reimbursement_Formula);
			blk.Specialty_Single_Generic_Reimbursement_Formula = pStmt->GetInt("Specialty_Single_Generic_Reimbursement_Formula", blk.Specialty_Single_Generic_Reimbursement_Formula);
			blk.Specialty_Multi_Generic_Reimbursement_Formula = pStmt->GetInt("Specialty_Multi_Generic_Reimbursement_Formula", blk.Specialty_Multi_Generic_Reimbursement_Formula);
			blk.SpecialtyNextFillAllowedAfterCash = pStmt->GetInt("SpecialtyNextFillAllowedAfterCash", blk.SpecialtyNextFillAllowedAfterCash);			// 01/06/15 Troy TP#28029
			blk.SpecialtyNextFillAllowedAfterInsured = pStmt->GetInt("SpecialtyNextFillAllowedAfterInsured", blk.SpecialtyNextFillAllowedAfterInsured);	// 01/06/15 Troy TP#28029
			blk.MinimumDaysBetweenPaidClaimsCash = pStmt->GetInt("MinimumDaysBetweenPaidClaimsCash", blk.MinimumDaysBetweenPaidClaimsCash);		// 12/09/15 Igor TP#30762
			blk.MinimumDaysBetweenPaidClaimsInsured = pStmt->GetInt("MinimumDaysBetweenPaidClaimsInsured", blk.MinimumDaysBetweenPaidClaimsInsured);	// 12/09/15 Igor TP#30762
			// 10/27/15 Igor TP#30346
			blk.MaxBenefitInsNotCovered = pStmt->GetNumeric("MaxBenefitInsNotCovered", blk.MaxBenefitInsNotCovered);
			blk.MailMaxBenefitInsNotCovered = pStmt->GetNumeric("MailMaxBenefitInsNotCovered", blk.MailMaxBenefitInsNotCovered);
			blk.SpecialtyMaxBenefitInsNotCovered = pStmt->GetNumeric("SpecialtyMaxBenefitInsNotCovered", blk.SpecialtyMaxBenefitInsNotCovered);
			blk.MinPatPayAmtInsNotCovered = pStmt->GetNumeric("MinPatPayAmtInsNotCovered", blk.MinPatPayAmtInsNotCovered);
			blk.MailMinPatPayAmtInsNotCovered = pStmt->GetNumeric("MailMinPatPayAmtInsNotCovered", blk.MailMinPatPayAmtInsNotCovered);
			blk.SpecialtyMinPatPayAmtInsNotCovered = pStmt->GetNumeric("SpecialtyMinPatPayAmtInsNotCovered", blk.SpecialtyMinPatPayAmtInsNotCovered);
			blk.PercentForInsNotCovered = pStmt->GetInt("PercentForInsNotCovered", blk.PercentForInsNotCovered);
			blk.EnrollmentLookBackDays = pStmt->GetInt("EnrollmentLookBackDays", blk.EnrollmentLookBackDays);		// 11/24/15 Igor TP#30671
			blk.ReEnrollmentLookBackDays = pStmt->GetInt("ReEnrollmentLookBackDays", blk.ReEnrollmentLookBackDays);	// 11/24/15 Igor TP#30672
			blk.MaxBenefitIndividualRule = pStmt->GetInt("MaxBenefitIndividualRule", blk.MaxBenefitIndividualRule);	// 12/04/15 Igor TP#30673
			if (!pStmt->IsNull("EnrollmentDays")) // 10/14/20 Igor TP#44788 - added validation for NULL value
			{
				blk.ProcessEnrollment = TRUE;
				blk.EnrollmentDays = pStmt->GetInt("EnrollmentDays", blk.EnrollmentDays);	// 12/07/15 Igor TP#30673
			}
			else
			{
				blk.ProcessEnrollment = FALSE;
				blk.EnrollmentDays = 0;
			}
			blk.VariableBenefitRuleCash = pStmt->GetInt("VariableBenefitRuleCash", blk.VariableBenefitRuleCash);		// 12/21/15 Troy TP#30673
			blk.VariableBenefitRuleInsured = pStmt->GetInt("VariableBenefitRuleInsured", blk.VariableBenefitRuleInsured);		// 12/21/15 Troy TP#30673
			pStmt->GetChar("MinimumDaysBetweenPaidClaimsCashOverrideType", &blk.MinimumDaysBetweenPaidClaimsCashOverrideType);	// 02/08/16 Igor TP#31210
			pStmt->GetChar("MinimumDaysBetweenPaidClaimsInsuredOverrideType", &blk.MinimumDaysBetweenPaidClaimsInsuredOverrideType); // 02/08/16 Igor TP#31210
			blk.AncillaryBusinessRules = pStmt->GetInt("AncillaryBusinessRules", blk.AncillaryBusinessRules); // 02/25/16 Igor TP#31379

			//01/05/16 Igor TP#33350 - added 67 missing fields
			blk.Mail_Maximum_Metric_Qty = pStmt->GetNumeric("MAIL_MAXIMUM_METRIC_QTY", blk.Mail_Maximum_Metric_Qty);
			blk.Maximum_Metric_Qty = pStmt->GetNumeric("MAXIMUM_METRIC_QTY", blk.Maximum_Metric_Qty);
			blk.Maximum_Unit_Qty = pStmt->GetNumeric("MAXIMUM_UNIT_QTY", blk.Maximum_Unit_Qty);
			blk.Mail_Maximum_Days_Supply = pStmt->GetInt("MAIL_MAXIMUM_DAYS_SUPPLY", blk.Mail_Maximum_Days_Supply);
			blk.Maximum_Days_Supply = pStmt->GetInt("MAXIMUM_DAYS_SUPPLY", blk.Maximum_Days_Supply);
			blk.Days_Since_Written = pStmt->GetInt("DAYS_SINCE_WRITTEN", blk.Days_Since_Written);
			blk.Days_Since_Filled = pStmt->GetInt("DAYS_SINCE_FILLED", blk.Days_Since_Filled);
			blk.Rule_ID = pStmt->GetInt("Rule_ID", blk.Rule_ID);
			pStmt->GetString("RxDenialOverridesAllowed", blk.RxDenialOverridesAllowed, sizeof(blk.RxDenialOverridesAllowed), blk.RxDenialOverridesAllowed);
			pStmt->GetString("ValidStatusCodes", blk.ValidStatusCodes, sizeof(blk.ValidStatusCodes), blk.ValidStatusCodes);
			blk.MaximumBenefitIndividual = pStmt->GetNumeric("Maximum_Benefit_Individual", blk.MaximumBenefitIndividual);
			blk.MaximumBenefitFamily = pStmt->GetNumeric("Maximum_Benefit_Family", blk.MaximumBenefitFamily);
			pStmt->GetChar("IgnoreIndividMaxBenefitWhenFamilyExists", &blk.IgnoreIndividualBenefitWhenFamilyExists, blk.IgnoreIndividualBenefitWhenFamilyExists);
			blk.DebitRxProcessor = pStmt->GetInt("DebitRxProcessor", blk.DebitRxProcessor);
			blk.Mail_Minimum_Days_Supply = pStmt->GetInt("Mail_Minimum_Days_Supply", blk.Mail_Minimum_Days_Supply);
			blk.Minimum_Days_Supply = pStmt->GetInt("Minimum_Days_Supply", blk.Minimum_Days_Supply);
			blk.MedicareChecking = pStmt->GetInt("MedicareChecking", blk.MedicareChecking);
			pStmt->GetChar("MedicareLevelOfChecking", &blk.MedicareLevelOfChecking, blk.MedicareLevelOfChecking);
			pStmt->GetString("MedicareRspMsg", blk.MedicareRspMsg, sizeof(blk.MedicareRspMsg), blk.MedicareRspMsg);
			pStmt->GetString("MailRxDenialOverridesAllowed", blk.MailRxDenialOverridesAllowed, sizeof(blk.MailRxDenialOverridesAllowed), blk.MailRxDenialOverridesAllowed);
			blk.Maximum_Refills = pStmt->GetNumeric("MailMaximumRefills", blk.Maximum_Refills);
			blk.MinimumAge = pStmt->GetInt("MinimumAge", blk.MinimumAge);
			blk.NextFillAllowedAfterRule = pStmt->GetInt("NextFillAllowedAfterRule", blk.NextFillAllowedAfterRule);
			pStmt->GetString("MedicarePaidMessage", blk.MedicarePaidMessage, sizeof(blk.MedicarePaidMessage), blk.MedicarePaidMessage);
			pStmt->GetString("MedicareRejectMessage", blk.MedicareRejectMessage, sizeof(blk.MedicareRejectMessage), blk.MedicareRejectMessage);
			blk.MultiBenefitRule = pStmt->GetInt("MultiBenefitRule", blk.MultiBenefitRule);
			blk.MailMultiBenefitRule = pStmt->GetInt("MailMultiBenefitRule", blk.MailMultiBenefitRule);
			pStmt->GetChar("MaxIndividBenefitExceededBehavior", &blk.MaxIndividBenefitExceededBehavior, blk.MaxIndividBenefitExceededBehavior);
			blk.Minimum_Metric_Qty = pStmt->GetNumeric("Minimum_Metric_Qty", blk.Minimum_Metric_Qty);
			blk.Mail_Minimum_Metric_Qty = pStmt->GetNumeric("Mail_Minimum_Metric_Qty", blk.Mail_Minimum_Metric_Qty);
			blk.NextFillAllowedAfterCash = pStmt->GetInt("NextFillAllowedAfterCash", blk.NextFillAllowedAfterCash);
			blk.NextFillAllowedAfterInsured = pStmt->GetInt("NextFillAllowedAfterInsured", blk.NextFillAllowedAfterInsured);
			blk.MailNextFillAllowedAfterCash = pStmt->GetInt("MailNextFillAllowedAfterCash", blk.MailNextFillAllowedAfterCash);
			blk.MailNextFillAllowedAfterInsured = pStmt->GetInt("MailNextFillAllowedAfterInsured", blk.MailNextFillAllowedAfterInsured);
			blk.EnforceMinPatPayAmtMultiBenefitRules = pStmt->GetInt("EnforceMinPatPayAmtMultiBenefitRules", blk.EnforceMinPatPayAmtMultiBenefitRules);
			blk.InsuranceVerificationRule = pStmt->GetInt("InsuranceVerificationRule", blk.InsuranceVerificationRule);
			blk.InsuranceVerificationLookbackPeriod = pStmt->GetInt("InsuranceVerificationLookbackPeriod", blk.InsuranceVerificationLookbackPeriod);
			blk.InsuranceVerificationUniquePARequired = pStmt->GetInt("InsuranceVerificationUniquePARequired", blk.InsuranceVerificationUniquePARequired);
			blk.CustomRejectRules = pStmt->GetInt("CustomRejectRules", blk.CustomRejectRules);
			blk.StepEditRules = pStmt->GetInt("StepEditRules", blk.StepEditRules);
			blk.OtherPayerBINTypeRules = pStmt->GetInt("OtherPayerBINTypeRules", blk.OtherPayerBINTypeRules);
			blk.PrimaryRejectCodesAction = pStmt->GetInt("PrimaryRejectCodesAction", blk.PrimaryRejectCodesAction);
			blk.MedicareVerificationRule = pStmt->GetInt("MedicareVerificationRule", blk.MedicareVerificationRule);
			blk.MedicareVerificationLookbackPeriod = pStmt->GetInt("MedicareVerificationLookbackPeriod", blk.MedicareVerificationLookbackPeriod);
			blk.MedicareVerificationUniquePARequired = pStmt->GetInt("MedicareVerificationUniquePARequired", blk.MedicareVerificationUniquePARequired);
			blk.MedicareOverrideRule = pStmt->GetInt("MedicareOverrideRule", blk.MedicareOverrideRule);
			blk.MedicareOverridePeriod = pStmt->GetInt("MedicareOverridePeriod", blk.MedicareOverridePeriod);
			blk.MedicareOverrideScope = pStmt->GetInt("MedicareOverrideScope", blk.MedicareOverrideScope);
			blk.MedicareOverrideDateType = pStmt->GetInt("MedicareOverrideDateType", blk.MedicareOverrideDateType);
			blk.GovtCoverageAttestationRule = pStmt->GetInt("GovtCoverageAttestationRule", blk.GovtCoverageAttestationRule);
			blk.GovtCoverageAttestationValidateDays = pStmt->GetInt("GovtCoverageAttestationValidateDays", blk.GovtCoverageAttestationValidateDays);
			blk.GovtCoverageAttestationValidateAge = pStmt->GetInt("GovtCoverageAttestationValidateAge", blk.GovtCoverageAttestationValidateAge);
			blk.MinPatPayAmtRule = pStmt->GetInt("MinPatPayAmtRule", blk.MinPatPayAmtRule);
			blk.MinPatPayCopayPercentageCash = pStmt->GetInt("MinPatPayCopayPercentageCash", blk.MinPatPayCopayPercentageCash);
			blk.MinPatPayCopayPercentageInsured = pStmt->GetInt("MinPatPayCopayPercentageInsured", blk.MinPatPayCopayPercentageInsured);
			blk.MailMinPatPayCopayPercentageCash = pStmt->GetInt("MailMinPatPayCopayPercentageCash", blk.MailMinPatPayCopayPercentageCash);
			blk.MailMinPatPayCopayPercentageInsured = pStmt->GetInt("MailMinPatPayCopayPercentageInsured", blk.MailMinPatPayCopayPercentageInsured);
			blk.SpecialtyMinPatPayCopayPercentageCash = pStmt->GetInt("SpecialtyMinPatPayCopayPercentageCash", blk.SpecialtyMinPatPayCopayPercentageCash);
			blk.SpecialtyMinPatPayCopayPercentageInsured = pStmt->GetInt("SpecialtyMinPatPayCopayPercentageInsured", blk.SpecialtyMinPatPayCopayPercentageInsured);
			pStmt->GetString("CalculateBenefitSP", blk.VariableBenefitCustomSP, sizeof(blk.VariableBenefitCustomSP), blk.VariableBenefitCustomSP);
			blk.MaxDosagePerDay = pStmt->GetNumeric("MaxDosagePerDay", blk.MaxDosagePerDay);
			pStmt->GetString("CustomCPLSP", blk.CustomCPLSP, sizeof(blk.CustomCPLSP), blk.CustomCPLSP);
			blk.GovtCoverageCopayThreshold = pStmt->GetNumeric("GovtCoverageCopayThreshold", blk.GovtCoverageCopayThreshold);
			pStmt->GetString("CustomDrugRestrictionSP", blk.DrugRestrictionCustomSP, sizeof(blk.DrugRestrictionCustomSP), blk.DrugRestrictionCustomSP);
			pStmt->GetString("CustomLNSCardValidationSP", blk.LNSCardValidationCustomSP, sizeof(blk.LNSCardValidationCustomSP), blk.LNSCardValidationCustomSP);
			blk.InsuredStatusOverrideRule = pStmt->GetInt("InsuredStatusOverrideRule", blk.InsuredStatusOverrideRule);
			blk.GovernmentCoverageRules = pStmt->GetInt("GovernmentCoverageRules", blk.GovernmentCoverageRules);
			blk.AdditionalMaximumBenefitIndividual = pStmt->GetNumeric("Additional_Maximum_Benefit_Individual", blk.AdditionalMaximumBenefitIndividual);	// 03/23/2018 Igor TP#37951
			pStmt->GetString("Additional_Maximum_Benefit_Individual_Status_Codes", blk.AdditionalMaximumBenefitIndividualStatusCodes, sizeof(blk.AdditionalMaximumBenefitIndividualStatusCodes), blk.AdditionalMaximumBenefitIndividualStatusCodes); // 03/23/2018 Igor TP#37951
			blk.CPL_Rule = pStmt->GetInt("MetaphoneMatching", blk.CPL_Rule); // 01/08/19 Igor TP#40205
			blk.MetaPhoneLinkID = pStmt->GetInt("MetaPhoneLinkID", blk.MetaPhoneLinkID); // 01/08/19 Igor TP#40205
			blk.TokenBenefitMultiplierValue = pStmt->GetInt("TokenBenefitMultiplierValue", blk.TokenBenefitMultiplierValue); // 03/10/19 Igor TP#40421 
			blk.TokenBenefitDollarDigitCount = pStmt->GetInt("TokenBenefitDollarDigitCount", blk.TokenBenefitDollarDigitCount); // 03/10/19 Igor TP#40421 
			// 04/05/19 Igor TP#40761 
			blk.MedicaidVerificati