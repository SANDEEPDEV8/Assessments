int ProcessAccumulatedBenefits(RX_Message_struct* pRX_Message, ClaimAmount_Info &Amount_blk, double& CouponAmt, double& PatientPayAmount, double& AdditionalMaximumBenefitApplied, // 04/09/18 Igor TP#38034 - added AdditionalMaximumBenefitApplied
	double* pOverTheLimitForReversal, int RolloverMaximizerScriptLimitReached) // 11/06/19 Igor TP#41895 added OverTheLimit // 07/14/20 Igor TP#44421 - converted double& OverTheLimit to double* pOverTheLimitForReversal // 12/15/23 Troy OCLE-4863 - Added RolloverMaximizerScriptLimitReached
{
	Reporter reporter(MonitorFile, "ProcessAccumulatedBenefits");

	int FamilyCount = 0;

	if (Group_blk.IgnoreIndividualBenefitWhenFamilyExists == 'Y')
		FamilyCount = Get_NumPatientsInFamily(Claimant_blk.MemberNumber, Group_blk.Unique_ID, FALSE); //06/14/12 Igor - modified function parameters for TP#20022
	else
		FamilyCount = -1;

	if (Group_blk.MaximumBenefitIndividual > 0 && (FamilyCount == 1 || FamilyCount == -1))
	{
		int retValue = 0;
		int Tier1BenefitCapReached = 0;		// 04/07/23 Troy OCLE-4838

		// 12/12/22 Igor OCLE-4639
		if (Group_blk.TieredAnnualCapRule > 0
			&& ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) != BDM_MAXIMIZER) // not marked as maximizer
			&& Group_blk.Tier1MaximumBenefitIndividual > 0)
		{
		// 12/30/23 Troy OCLE-5144 - Changed TieredAnnualCapRule > 0 to use TAC_T1_DISP_THRESHOLD flag
		/*
		if (((Group_blk.TieredAnnualCapRule & TAC_T1_DISP_THRESHOLD) == TAC_T1_DISP_THRESHOLD)
			&& ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) != BDM_MAXIMIZER) // not marked as maximizer
			&& Group_blk.Tier1MaximumBenefitIndividual > 0)
		{
		*/
			double AccumulatedForTier1 = 0.0;
			int ClaimCount = Group_blk.Tier1AnnualCapDispensingThreshold;
			retValue = GetAccumulatedBenefits(AccumulatedForTier1, ClaimCount, pRX_Message->DateFilled);

			if (retValue < 0)
			{
				reporter.error("GetAccumulatedBenefits returns SQL Error");

				Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 302);
				return RETURN_FAILURE;
			}
			// 02/02/24 Troy OCLE-5262
			else
			{
				if ((Group_blk.TieredAnnualCapRule & TAC_T1_IGNORE_1_TIME_PA_FROM_CLAIM_COUNT) == TAC_T1_IGNORE_1_TIME_PA_FROM_CLAIM_COUNT)
				{
					int TempClaimCount = Group_blk.Tier1AnnualCapDispensingThreshold;
					retValue = GetAccumulatedClaimCount(TempClaimCount, pRX_Message->DateFilled, TRUE);
					if (retValue < 0)
					{
						reporter.error("GetAccumulatedClaimCount returns SQL Error");
						Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 366);
						return RETURN_FAILURE;
					}
					else
						ClaimCount = TempClaimCount;
				}

				// 03/24/23 Igor OCLE-4805
				if (AccumulatedForTier1 >= Group_blk.Tier1MaximumBenefitIndividual &&
					ClaimCount >= Group_blk.Tier1AnnualCapDispensingThreshold)
				{
					// 04/10/23 Troy OCLE-4838
					reporter.info("Patient has used Tier 1 Benefit amount of <%.2f> within their first <%d> claims", AccumulatedForTier1,
						Group_blk.Tier1AnnualCapDispensingThreshold);

					Tier1BenefitCapReached = 1;
					/*
					if (pPAStandard)
					{
						reporter.info("PA <%s> type <%s> allows to override Tier 1 Maximum Individual Benefits with <%.2f>", pPAStandard->Number, pPAStandard->OpusType, Group_blk.MaximumBenefitIndividual);
					}
					else
					{
						// 04/10/23 Troy OCLE-4838 - Check for previously used SMB PA
						PriorAuthorization pa;
						char fillDate[PACKED_DATE_SIZE] = { 0 };
						retValue = ValidatePreviouslyUsedPA("SMB", &pa, fillDate, sizeof(fillDate));

						if (retValue == SQL_PROCESSING_ERROR)
						{
							reporter.error("ValidatePreviouslyUsedPA returns SQL error");

							Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 319);
							return SQL_PROCESSING_ERROR;
						}
						else if (retValue == RETURN_SUCCESS)
						{
							reporter.info("Previously used on <%2.2s/%2.2s/%2.2s> PA <%s> type <%s> allows to apply Standard Maximum Benefits of <%.2f>",
								fillDate + 4, fillDate + 6, fillDate + 2,
								pa.Number, pa.OpusType, Group_blk.MaximumBenefitIndividual);
						}
						else
						{
							Group_blk.MaximumBenefitIndividual = 0;
							Group_blk.BenefitDeterminationMethod |= BDM_TIERED;
						}
					}
					*/
				}
				else if //(retValue == RETURN_SUCCESS // 03/22/23 Igor OCLE-4793 - should process even when no records were found
					(AccumulatedForTier1 + CouponAmt >= Group_blk.Tier1MaximumBenefitIndividual  // 03/17/23 Igor OCLE-4639
						&& ClaimCount < Group_blk.Tier1AnnualCapDispensingThreshold)
				{
					// 04/10/23 Troy OCLE-4838
					reporter.info("Applied Tier 1 Maximum Benefit amount of <%.2f>", Group_blk.Tier1MaximumBenefitIndividual);

					Tier1BenefitCapReached = 2;
					/*
					if (pPAStandard)
					{
						reporter.info("PA <%s> type <%s> allows to override Tier 1 Maximum Individual Benefits with <%.2f>", pPAStandard->Number, pPAStandard->OpusType, Group_blk.MaximumBenefitIndividual);
					}
					else
					{
						// 04/10/23 Troy OCLE-4838 - Check for previously used SMB PA
						PriorAuthorization pa;
						char fillDate[PACKED_DATE_SIZE] = { 0 };
						retValue = ValidatePreviouslyUsedPA("SMB", &pa, fillDate, sizeof(fillDate));

						if (retValue == SQL_PROCESSING_ERROR)
						{
							reporter.error("ValidatePreviouslyUsedPA returns SQL error");

							Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 319);
							return SQL_PROCESSING_ERROR;
						}
						else if (retValue == RETURN_SUCCESS)
						{
							reporter.info("Previously used on <%2.2s/%2.2s/%2.2s> PA <%s> type <%s> allows to apply Standard Maximum Benefits of <%.2f>",
								fillDate + 4, fillDate + 6, fillDate + 2,
								pa.Number, pa.OpusType, Group_blk.MaximumBenefitIndividual);
						}
						else
						{
							Group_blk.MaximumBenefitIndividual = Group_blk.Tier1MaximumBenefitIndividual;
							Group_blk.BenefitDeterminationMethod |= BDM_TIERED;
						}
					}
					*/
				}
				// 12/30/23 Troy OCLE-5144
				else if (((Group_blk.TieredAnnualCapRule & TAC_T1_REJECT_ELEC_BEFORE_BV) == TAC_T1_REJECT_ELEC_BEFORE_BV)
					&& pRX_Message->Status == ELECTRONIC && pRX_Message->ActivityType == NORMAL)
				{
					int BenefitVerificationPerformed = 0, PriorRejectCountForAnnualCap = 0;
					char BenefitVerificationDate[PACKED_DATE_SIZE] = { 0 }, EarliestAnnualCapRejectDate[PACKED_DATE_SIZE] = { 0 };

					retValue = GetBenefitVerificationAndAnnualCapRejects(BenefitVerificationPerformed, BenefitVerificationDate, sizeof(BenefitVerificationDate), PriorRejectCountForAnnualCap, EarliestAnnualCapRejectDate, sizeof(EarliestAnnualCapRejectDate), ANNUAL_CAP_TIER1, Claimant_blk, pRX_Message);

					if (retValue == SQL_PROCESSING_ERROR)
					{
						reporter.error("GetBenefitVerificationAndAnnualCapRejects (Tier1) returns SQL error");

						Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 362);
						return SQL_PROCESSING_ERROR;
					}
					else if (retValue == RETURN_SUCCESS)
					{
						if (PriorRejectCountForAnnualCap && !BenefitVerificationPerformed)
						{
							// There was a prior reject, but no BV was performed yet.  Check for PA, and if not, reject.
							PriorAuthorization* bvPA = UniquePA(pRX_Message->Claim.PAArray, "BVT1");
							if (bvPA)
							{
								reporter.info("PA <%s> type <%s> allows to override Tier 1 Annual Cap Rejection without a BV being performed", bvPA->Number, bvPA->OpusType);
							}
							else
							{
								reporter.warn("Patient has prior rejection for Tier 1 Annual Cap and no BV has been performed yet.");

								Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 21);
								return RETURN_FAILURE;
							}
						}
						else if (PriorRejectCountForAnnualCap && BenefitVerificationPerformed)
						{
							// There was a prior reject but a BV has already been performed.
							reporter.info("Patient has prior rejection for Tier 1 Annual Cap but a BV has been performed.");
						}
					}
				}

			}
		}

		double AccumulatedBenefitIndividual = 0;
		
		// 02/02/24 Troy OCLE-5207 - added NULL for last parameter
		retValue = AccumulatedBenefits(AccumulatedBenefitIndividual, Claimant_blk, pRX_Message, TRUE, NULL); // individual
		
		// 02/02/24 Troy OCLE-5207 - Adding output of accumulated benefit.  Good to have in the log anyway.
		reporter.info("Accumulated Benefit <%.2f>", AccumulatedBenefitIndividual);

		if (retValue == SQL_PROCESSING_ERROR) // 09/01/17 Igor TP#36039
		{
			reporter.error("AccumulatedBenefits returns SQL error");

			Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 136);
			return SQL_PROCESSING_ERROR;
		}
		// 01/11/24 Troy OCLE-5170
		else if (retValue == GENERAL_PROCESSING_ERROR)  // 01/25/24 Troy OCLE-5251 - changed from RETURN_FAILURE to GENERAL_PROCESSING_ERROR
		{
			reporter.error("AccumulatedBenefits returns unexpected error");

			Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 364);
			return SQL_PROCESSING_ERROR;
		}

		// 04/25/23 Troy OCLE-4874
		int SMBPriorAuthUsed = 0;
		int EMBPriorAuthUsed = 0;

		// 04/07/23 Troy OCLE-4838 - Implement SMB Prior Auth which instructs the Engine to use the Maximum_Benefit_Individual
		if (Tier1BenefitCapReached)
		{
			PriorAuthorization* pPAStandard = UniquePA(pRX_Message->Claim.PAArray, "SMB");
			if (pPAStandard)
			{
				if (Group_blk.MaximumBenefitIndividual == -1)
				{
					reporter.error("Unsupported configuration - M_Plan.Maximum_Benefit_Individual should be set to use with PA <%s> type <%s>", pPAStandard->Number, pPAStandard->OpusType);

					Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 318);
					return RETURN_FAILURE;
				}
				else
				{
					reporter.info("PA <%s> type <%s> allows to override Tier 1 Maximum Individual Benefits with <%.2f>", pPAStandard->Number, pPAStandard->OpusType, Group_blk.MaximumBenefitIndividual);
					SMBPriorAuthUsed = 1; // 04/25/23 Troy OCLE-4874
				}
			}
			else
			{
				PriorAuthorization pa;
				char fillDate[PACKED_DATE_SIZE] = { 0 };
				retValue = ValidatePreviouslyUsedPA("SMB", &pa, Claimant_blk.PatientBenefitStartDate, Claimant_blk.PatientBenefitEndDate, fillDate, sizeof(fillDate)); // 01/08/24 Troy OCLE-5154 - Added StartFillDate and EndFillDate

				if (retValue == SQL_PROCESSING_ERROR)
				{
					reporter.error("ValidatePreviouslyUsedPA returns SQL error");

					Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 319);
					return SQL_PROCESSING_ERROR;
				}
				else if (retValue == RETURN_SUCCESS)
				{
					reporter.info("Previously used on <%2.2s/%2.2s/%2.2s> PA <%s> type <%s> allows to apply Standard Maximum Benefits of <%.2f>",
						fillDate + 4, fillDate + 6, fillDate + 2,
						pa.Number, pa.OpusType, Group_blk.MaximumBenefitIndividual);
					SMBPriorAuthUsed = 2; // 04/25/23 Troy OCLE-4874
				}
				else
				{
					if (Tier1BenefitCapReached == 1)
					{
						Group_blk.MaximumBenefitIndividual = 0;
						Group_blk.BenefitDeterminationMethod |= BDM_TIERED;
					}
					else if (Tier1BenefitCapReached == 2)
					{
						Group_blk.MaximumBenefitIndividual = Group_blk.Tier1MaximumBenefitIndividual;
						Group_blk.BenefitDeterminationMethod |= BDM_TIERED;

						// 12/11/23 Troy OCLE-5040
						if (Group_blk.Tier1MaxBenefitIndividualRule && Group_blk.Tier1MaxBenefitIndividualRule != Group_blk.MaxBenefitIndividualRule)
						{
							reporter.info("MaxBenefitIndividualRule <%ld> overridden with Tier1MaxBenefitIndividualRule <%ld>",
								Group_blk.MaxBenefitIndividualRule, Group_blk.Tier1MaxBenefitIndividualRule);

							Group_blk.MaxBenefitIndividualRule = Group_blk.Tier1MaxBenefitIndividualRule;
						}
					}
				}
			}
		}

		// 01/10/24 Troy OCLE-5144
		if (((Group_blk.StandardAnnualCapOptions & SAC_REJECT_ELEC_BEFORE_BV) == SAC_REJECT_ELEC_BEFORE_BV)
			&& pRX_Message->Status == ELECTRONIC && pRX_Message->ActivityType == NORMAL)
		{
			int BenefitVerificationPerformed = 0, PriorRejectCountForAnnualCap = 0;
			char BenefitVerificationDate[PACKED_DATE_SIZE] = { 0 }, EarliestAnnualCapRejectDate[PACKED_DATE_SIZE] = { 0 };

			retValue = GetBenefitVerificationAndAnnualCapRejects(BenefitVerificationPerformed, BenefitVerificationDate, sizeof(BenefitVerificationDate), PriorRejectCountForAnnualCap, EarliestAnnualCapRejectDate, sizeof(EarliestAnnualCapRejectDate), ANNUAL_CAP_STANDARD, Claimant_blk, pRX_Message);

			if (retValue == SQL_PROCESSING_ERROR)
			{
				reporter.error("GetBenefitVerificationAndAnnualCapRejects (Standard) returns SQL error");

				Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 363);
				return SQL_PROCESSING_ERROR;
			}
			else if (retValue == RETURN_SUCCESS)
			{
				if (PriorRejectCountForAnnualCap && !BenefitVerificationPerformed)
				{
					// There was a prior reject, but no BV was performed yet.  Check for PA, and if not, reject.
					PriorAuthorization* bvPA = UniquePA(pRX_Message->Claim.PAArray, "BVS");
					if (bvPA)
					{
						reporter.info("PA <%s> type <%s> allows to override Standard Annual Cap Rejection without a BV being performed", bvPA->Number, bvPA->OpusType);
					}
					else
					{
						reporter.warn("Patient has prior rejection for Standard Annual Cap and no BV has been performed yet.");

						Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 22);
						return RETURN_FAILURE;
					}
				}
				else if (PriorRejectCountForAnnualCap && BenefitVerificationPerformed)
				{
					// There was a prior reject but a BV has already been performed.
					reporter.info("Patient has prior rejection for Standard Annual Cap but a BV has been performed.");
				}
			}
		}

		// 01/28/22 Igor TP#49388
		// BOOL useExpandedMaximumBenefitIndividual = FALSE; // 02/11/22 Igor TP#49513 - introduced BDM_EXPANDED value for BenefitDeterminationMethod

		PriorAuthorization* pPA = UniquePA(pRX_Message->Claim.PAArray, "EMB"); // 12/23/21 Igor TP#49267 - this PA instructs the engine to use ExpandedMaximumBenefitIndividual
		if (pPA)
		{
			if (Group_blk.ExpandedMaximumBenefitIndividual == -1)
			{
				reporter.error("Unsupported configuration - M_Plan.ExpandedMaximumBenefitIndividual should be set to use with PA <%s> type <%s>", pPA->Number, pPA->OpusType);

				Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 135);
				return RETURN_FAILURE;
			}
			// 11/30/23 Troy OCLE-4738
			else if (!Group_blk.ExpandedAnnualCapRule)
			{
				reporter.error("Unsupported configuration - M_Plan.ExpandedAnnualCapRule should be set to use with PA <%s> type <%s>", pPA->Number, pPA->OpusType);

				Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 348);
				return RETURN_FAILURE;
			}
			else
			{
				// 08/01/23 Troy OCLE-4928
				// 01/31/24 Troy OCLE-5204 - Added MaximizerPriorAuthRule
				if (((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER) &&
					(((Group_blk.MaximizerRule & MR_BLOCK_EXPANDED_CAP) == MR_BLOCK_EXPANDED_CAP) || ((Group_blk.MaximizerPriorAuthRule & MAXIMIZER_PA_BLOCK_EMB) == MAXIMIZER_PA_BLOCK_EMB)))
				{
					// 01/31/24 Troy OCLE-5204 - use different qualifiers based on the rule
					if ((Group_blk.MaximizerRule & MR_BLOCK_EXPANDED_CAP) == MR_BLOCK_EXPANDED_CAP)
					{
						// 08/01/23 Troy OCLE-4928
						reporter.info("PA <%s> type <%s> to allow Expanded Annual Cap has been blocked based on Maximizer Rule", pPA->Number, pPA->OpusType);
						Send_Reject(pRX_Message, Reject_1J_Benefit_Not_Effective, 2);
						Send_Reject(pRX_Message, Reject_3Y_Prior_Auth_Denied, 2);
					}
					else
					{
						// 01/31/24 Troy OCLE-5204
						reporter.info("PA <%s> type <%s> to allow Expanded Annual Cap has been blocked based on Maximizer Prior Auth Rule", pPA->Number, pPA->OpusType);
						Send_Reject(pRX_Message, Reject_1J_Benefit_Not_Effective, 6);
						Send_Reject(pRX_Message, Reject_3Y_Prior_Auth_Denied, 5);
					}
						return RETURN_FAILURE;
				}
				else
				{
					reporter.info("PA <%s> type <%s> allows to override Maximum Individual Benefits with <%.2f>", pPA->Number, pPA->OpusType, Group_blk.ExpandedMaximumBenefitIndividual);
					// useExpandedMaximumBenefitIndividual = TRUE;
					Group_blk.BenefitDeterminationMethod |= BDM_EXPANDED; // set to override standard Maximum_Benefit_Individual with ExpandedMaximumBenefitIndividual 
					Group_blk.BenefitDeterminationMethod &= ~(BDM_TIERED); // 12/12/22 Igor OCLE-4639 - remove Tier1 flag when forced to ExpandedMaximumBenefitIndividual
					EMBPriorAuthUsed = 1; // 04/25/23 Troy OCLE-4874
				}
			}
		}

		// 12/23/21 Igor TP#49267 - at this point we have the Claimant_blk.PatientBenefitStartDate and Claimant_blk.PatientBenefitEndDate calculated
		// so checking for prior PA
		if (// 03/22/23 Igor OCLE-4799 - commented out to allow check for Expanded Benefits even if the patient was qualified for Tier1 benefits
			//((Group_blk.BenefitDeterminationMethod & BDM_TIERED) != BDM_TIERED) &&		// 12/12/22 Igor OCLE-4639
			((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) != BDM_EXPANDED)  // 02/11/22 Igor TP#49513 - added BDM_EXPANDED value for BenefitDeterminationMethod
			&& Group_blk.ExpandedMaximumBenefitIndividual > -1
			&& Group_blk.ExpandedAnnualCapRule) // 11/30/23 Troy OCLE-4738
		{
			// 04/07/23 Troy OCLE-4844 - only check for a previously used EMB PA if the business rules indicate it's allowed on this program
			// 04/25/23 Troy OCLE-4874 - Moved this condition a little further down so that we can still perform the lookback to determine if the EMB PA was previously used
			//if (Group_blk.ExpandedMaximumBenefitIndividualPriorAuthRule == 0)
			//{
				PriorAuthorization pa;
				char fillDate[PACKED_DATE_SIZE] = { 0 };
				retValue = ValidatePreviouslyUsedPA("EMB", &pa, Claimant_blk.PatientBenefitStartDate, Claimant_blk.PatientBenefitEndDate, fillDate, sizeof(fillDate)); // 01/08/24 Troy OCLE-5154 - Added StartFillDate and EndFillDate

				if (retValue == SQL_PROCESSING_ERROR) // 09/01/17 Igor TP#36039
				{
					reporter.error("ValidatePreviouslyUsedPA returns SQL error");

					Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 303);
					return SQL_PROCESSING_ERROR;
				}
				else if (retValue == RETURN_SUCCESS)
				{
					// 04/25/23 Troy OCLE-4874
					if (Group_blk.ExpandedMaximumBenefitIndividualPriorAuthRule == 0)
					{
						// 08/09/23 Troy OCLE-4928
						// 01/31/24 Troy OCLE-5204 - Added MaximizerPriorAuthRule
						if (((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER) && ((Group_blk.MaximizerRule & MR_BLOCK_EXPANDED_CAP) == MR_BLOCK_EXPANDED_CAP))
						{
							// 08/09/23 Troy OCLE-4928
							reporter.info("Previously used PA <%s> type <%s> has been blocked based on Maximizer Rule",
								pa.Number, pa.OpusType);
							Send_Reject(pRX_Message, Reject_1J_Benefit_Not_Effective, 4);
							return RETURN_FAILURE;
						}
						else
						{
							reporter.info("Previously used on <%2.2s/%2.2s/%2.2s> PA <%s> type <%s> allows to apply Expanded Maximum Benefits of <%.2f>", // 02/11/22 Igor TP#49513 - changed wording
								fillDate + 4, fillDate + 6, fillDate + 2,
								pa.Number, pa.OpusType, Group_blk.ExpandedMaximumBenefitIndividual);
							// useExpandedMaximumBenefitIndividual = TRUE; // set to override standard Maximum_Benefit_Individual with ExpandedMaximumBenefitIndividual 
							Group_blk.BenefitDeterminationMethod |= BDM_EXPANDED; // 02/11/22 Igor TP#49513 - added BDM_EXPANDED value for BenefitDeterminationMethod
							Group_blk.BenefitDeterminationMethod &= ~(BDM_TIERED); // 12/12/22 Igor OCLE-4639 - remove Tier1 flag when forced to ExpandedMaximumBenefitIndividual
							EMBPriorAuthUsed = 2; // 04/25/23 Troy OCLE-4874
						}
					}
					else
					{
						reporter.info("Previously used on <%2.2s/%2.2s/%2.2s> PA <%s> type <%s> not being applied to this claim due to Expanded Annual Cap Prior Auth Rule",
							fillDate + 4, fillDate + 6, fillDate + 2,
							pa.Number, pa.OpusType);
						EMBPriorAuthUsed = 3; // 04/25/23 Troy OCLE-4874
					}
				}
			//}
			
			// 04/07/23 Troy OCLE-4844 - if we still don't have BDM_EXPANDED, continue checking the other options
			// 11/30/23 Troy OCLE-4738 - Added ExpandedAnnualCapRule
			if (((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) != BDM_EXPANDED) && Group_blk.ExpandedAnnualCapRule && Group_blk.ExpandedMaximumBenefitIndividual > -1)
			{
				// 04/09/22 Troy TP#49841 - The EMB Prior Auth was not previously used (04/07/23 Troy OCLE-4844: or not allowed per the business rules), so check the ExpandedBenefitPatients table.
				/* // 11/20/23 Troy OCLE-5058
				char groupNumber[16] = { 0 };
				char memberNumber[31] = { 0 };
				char patientState[3] = { 0 };

				retValue = ValidateExpandedBenefitPatient(groupNumber, sizeof(groupNumber), memberNumber, sizeof(memberNumber), patientState, sizeof(patientState), submissionMethod); // 04/13/22 Igor TP#49879 - added patientState, moved [size] parameters
				*/
				
				// 11/20/23 Troy OCLE-5058
				char expandedBenefitReason[51] = { 0 };

				retValue = ValidateExpandedBenefitPatient(pRX_Message->State, pRX_Message->Status, expandedBenefitReason, sizeof(expandedBenefitReason));

				if (retValue == SQL_PROCESSING_ERROR)
				{
					reporter.error("ValidateExpandedBenefitPatient returns SQL error");

					Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 307);
					return SQL_PROCESSING_ERROR;
				}
				else if (retValue == RETURN_SUCCESS)
				{
					// 08/01/23 Troy OCLE-4928
					if (((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER) && ((Group_blk.MaximizerRule & MR_BLOCK_EXPANDED_CAP) == MR_BLOCK_EXPANDED_CAP))
					{
						// 08/01/23 Troy OCLE-4928
						reporter.info("Patient assigned to Expanded Benefit List based on %s to allow Expanded Annual Cap has been blocked based on Maximizer Rule",
							// 11/21/23 Troy OCLE-5058
							//	strlen(patientState) > 0 ? "Patient State" : strlen(memberNumber) > 0 ? "Member #" : "Unknown"
							expandedBenefitReason
						);

						Send_Reject(pRX_Message, Reject_1J_Benefit_Not_Effective, 3);
						return RETURN_FAILURE;
					}
					else
					{
						reporter.info("Patient assigned to Expanded Maximum Benefit Individual of <%.2f> through Expanded Benefit Patient List based on %s",
							Group_blk.ExpandedMaximumBenefitIndividual,
							// 11/21/23 Troy OCLE-5058
							//strlen(patientState) > 0 ? "Patient State" : strlen(memberNumber) > 0 ? "Member #" : "Unknown" 	// 04/13/22 Igor TP#49879 - added extra wording
							expandedBenefitReason
						);
						Group_blk.BenefitDeterminationMethod |= BDM_EXPANDED;
						Group_blk.BenefitDeterminationMethod &= ~(BDM_TIERED); // 12/12/22 Igor OCLE-4639 - remove Tier1 flag when forced to ExpandedMaximumBenefitIndividual
					}
				}
				else
				{
					pPA = UniquePA(pRX_Message->Claim.PAArray, "EMB1"); // 06/28/22 Igor TP#49946 - added One-Time Use Unique PA PA to Allow Expanded Benefit
					if (pPA)
					{
						// 08/01/23 Troy OCLE-4928
						// 01/31/24 Troy OCLE-5204 - Added MaximizerPriorAuthRule
						if (((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER) &&
							(((Group_blk.MaximizerRule & MR_BLOCK_EXPANDED_CAP) == MR_BLOCK_EXPANDED_CAP) || ((Group_blk.MaximizerPriorAuthRule & MAXIMIZER_PA_BLOCK_EMB) == MAXIMIZER_PA_BLOCK_EMB)))
						{
							// 01/31/24 Troy OCLE-5204 - use different qualifiers based on the rule
							if ((Group_blk.MaximizerRule & MR_BLOCK_EXPANDED_CAP) == MR_BLOCK_EXPANDED_CAP)
							{
								// 08/01/23 Troy OCLE-4928
								reporter.info("PA <%s> type <%s> to allow Expanded Annual Cap has been blocked based on Maximizer Rule", pPA->Number, pPA->OpusType);
								Send_Reject(pRX_Message, Reject_1J_Benefit_Not_Effective, 1);
								Send_Reject(pRX_Message, Reject_3Y_Prior_Auth_Denied, 1);
							}
							else
							{
								// 01/31/24 Troy OCLE-5204
								reporter.info("PA <%s> type <%s> to allow Expanded Annual Cap has been blocked based on Maximizer Prior Auth Rule", pPA->Number, pPA->OpusType);
								Send_Reject(pRX_Message, Reject_1J_Benefit_Not_Effective, 5);
								Send_Reject(pRX_Message, Reject_3Y_Prior_Auth_Denied, 4);
							}
							return RETURN_FAILURE;
						}
						else
						{
							reporter.info("PA <%s> type <%s> allows to override Maximum Individual Benefits with <%.2f>", pPA->Number, pPA->OpusType, Group_blk.ExpandedMaximumBenefitIndividual);
							Group_blk.BenefitDeterminationMethod |= BDM_EXPANDED; // set to override standard Maximum_Benefit_Individual with ExpandedMaximumBenefitIndividual 
							Group_blk.BenefitDeterminationMethod &= ~(BDM_TIERED); // 12/12/22 Igor OCLE-4639 - remove Tier1 flag when forced to ExpandedMaximumBenefitIndividual
						}
					}
				}
			}
		}

		// 12/15/23 Troy OCLE-4863
		if ((Group_blk.BenefitDeterminationMethod & BDM_TIERED) != BDM_TIERED
			&& (Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) != BDM_EXPANDED
			&& (Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER
			&& RolloverMaximizerScriptLimitReached)
		{
			reporter.warn("Rollover Maximizer Patient reached the script limit and has not been overridden.");
			Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 20);
			return RETURN_FAILURE;
		}

		if ((Group_blk.BenefitDeterminationMethod & BDM_TIERED) != BDM_TIERED // 12/12/22 Igor OCLE-4639
			&& (Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED // 02/11/22 Igor TP#49513 - added BDM_EXPANDED value for BenefitDeterminationMethod
			&& Group_blk.ExpandedAnnualCapRule // 11/30/23 Troy OCLE-4738
			&& Group_blk.ExpandedMaximumBenefitIndividual > 0)
		{
			Group_blk.MaximumBenefitIndividual = Group_blk.ExpandedMaximumBenefitIndividual;

			// 12/11/23 Troy OCLE-5040
			if (Group_blk.ExpandedMaxBenefitIndividualRule && Group_blk.ExpandedMaxBenefitIndividualRule != Group_blk.MaxBenefitIndividualRule)
			{
				reporter.info("MaxBenefitIndividualRule <%ld> overridden with ExpandedMaxBenefitIndividualRule <%ld>",
					Group_blk.MaxBenefitIndividualRule, Group_blk.ExpandedMaxBenefitIndividualRule);

				Group_blk.MaxBenefitIndividualRule = Group_blk.ExpandedMaxBenefitIndividualRule;
			}

			// 12/23/21 Igor TP#49388
			if (Group_blk.AdditionalExpMaxBenefitIndividualStatusCodes[0])
			{
				// match codes defined in M_Plan.AdditionalExpandedMaximumBenefitIndividualStatusCodes
				int var = 0, validCodes[100] = { 0 }, codeIndex = 0, allowed = 0;
				char *next_token;
				char seps[] = ",";
				char* token = strtok_s(Group_blk.AdditionalExpMaxBenefitIndividualStatusCodes, seps, &next_token);
				while (token != NULL)
				{
					if (sscanf_s(token, "%d", &var) > 0)
						validCodes[codeIndex++] = var;
					token = strtok_s(NULL, seps, &next_token);
				}

				for (int pos = 0; pos < codeIndex; pos++)
				{
					if (validCodes[pos] == pRX_Message->Status)
					{
						allowed = 1;
						break;
					}
				}

				if (allowed && Group_blk.AdditionalExpandedMaximumBenefitIndividual > 0)
				{
					reporter.info("Applied Additional Expanded Maximum Benefit amount of <%.2f>", Group_blk.AdditionalExpandedMaximumBenefitIndividual);

					Group_blk.MaximumBenefitIndividual += Group_blk.AdditionalExpandedMaximumBenefitIndividual;
				}
			}
		}

		// 02/02/24 Troy OCLE-5207
		double AccumulatedMaximizerBenefit = 0, AccumulatedBenefitIndividualSave = 0;

		if ((Group_blk.BenefitDeterminationMethod & BDM_TIERED) != BDM_TIERED
			&& (Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) != BDM_EXPANDED
			&& (Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER
			&& (Group_blk.MaximizerRule & MR_LIMIT_CALC_STATUS_CODES) == MR_LIMIT_CALC_STATUS_CODES
			&& Group_blk.MaximizerCalculationStatusCodes[0])
		{
			retValue = AccumulatedBenefits(AccumulatedMaximizerBenefit, Claimant_blk, pRX_Message, TRUE, Group_blk.MaximizerCalculationStatusCodes); // individual
			if (retValue == SQL_PROCESSING_ERROR)
			{
				reporter.error("AccumulatedBenefits(Maximizer) returns SQL error");

				Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 367);
				return SQL_PROCESSING_ERROR;
			}

			reporter.info("Accumulated Benefit <%.2f> for Maximizer Patient Limited to Submission Methods <%s>",
				AccumulatedMaximizerBenefit,
				Group_blk.MaximizerCalculationStatusCodes);

			AccumulatedBenefitIndividualSave = AccumulatedBenefitIndividual;
			AccumulatedBenefitIndividual = AccumulatedMaximizerBenefit;
		}
		else if ((Group_blk.BenefitDeterminationMethod & BDM_TIERED) != BDM_TIERED
			&& (Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) != BDM_EXPANDED
			&& (Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER
			&& (Group_blk.MaximizerRule & MR_LIMIT_CALC_STATUS_CODES) == MR_LIMIT_CALC_STATUS_CODES
			&& !Group_blk.MaximizerCalculationStatusCodes[0])
		{
			reporter.error("Maximizer Rule set to Limit Accumulated Benefit to Specific Submission Methods, but no Submission Methods are configured.");

			Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 368);
			return SQL_PROCESSING_ERROR;
		}

		double TotalMaxBenefits = Group_blk.MaximumBenefitIndividual + Claimant_blk.AnnualMaxBenefitAddtlAmt;

		if (Claimant_blk.AnnualMaxBenefitAddtlAmt)
		{
			reporter.info("Applied Annual Additional amount of <%.2f>",	Claimant_blk.AnnualMaxBenefitAddtlAmt);
		}

		// 12/03/18 Igor TP#39960 - added Balance Adjustments
		double TotalAdjustmentAmount = 0;
		retValue = GetPatientBenefitBalanceAdjustments(TotalAdjustmentAmount, Claimant_blk.PatientBenefitStartDate, Claimant_blk.PatientBenefitEndDate);

		if (retValue == SQL_PROCESSING_ERROR) // 07/03/19 Igor added missing error check
		{
			reporter.error("GetPatientBenefitBalanceAdjustments returns SQL error");

			Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 255);
			return SQL_PROCESSING_ERROR;
		}

		if (TotalAdjustmentAmount)
		{
			double current = AccumulatedBenefitIndividual;
			AccumulatedBenefitIndividual -= TotalAdjustmentAmount;
			if (AccumulatedBenefitIndividual < 0)
				AccumulatedBenefitIndividual = 0;

			if (current != AccumulatedBenefitIndividual)
				reporter.info("Accumulated Individual Benefits Balance of <%.2f> %s to <%.2f> based on PatientBenefitBalanceAdjustments <%s%.2f>",
					current,
					current > AccumulatedBenefitIndividual ? "reduced" : "increased",
					AccumulatedBenefitIndividual,
					TotalAdjustmentAmount > 0 ? "+" : "",
					TotalAdjustmentAmount);
		}

		// 11/22/13 Igor TP#24469
		double TotalAccumulated = AccumulatedBenefitIndividual + CouponAmt;

		// 03/22/18 Igor TP#37951
		if (Group_blk.AdditionalMaximumBenefitIndividual)
		{
			// 03/23/20 Igor TP#43458 - allow additional amount from CLAIMS.AdvancedRefillRules 
			if ((Group_blk.BenefitDeterminationMethod & BDM_ADVANCED_REFILL_RULES) == BDM_ADVANCED_REFILL_RULES)
			{
				TotalMaxBenefits += Group_blk.AdditionalMaximumBenefitIndividual;

				AdditionalMaximumBenefitApplied = Group_blk.AdditionalMaximumBenefitIndividual; // 04/09/18 Igor TP#38034 - added AdditionalMaximumBenefitApplied

				reporter.info("Program limit <%.2f> based on %sIndividual Maximum benefit of <%.2f> and Additional amount of <%.2f>",
					TotalMaxBenefits,
					Claimant_blk.AnnualMaxBenefitAddtlAmt ? "Adjusted " : "",
					Group_blk.MaximumBenefitIndividual + Claimant_blk.AnnualMaxBenefitAddtlAmt,
					Group_blk.AdditionalMaximumBenefitIndividual);
			}
			else
			{
				if (Group_blk.AdditionalMaximumBenefitIndividualRule == 1 // 09/23/21 Igor TP#48755
					|| (Group_blk.AdditionalMaximumBenefitIndividualRule == 2
						//&&  !Claimant_blk.MaximizerBenefit))
						&& ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) != BDM_MAXIMIZER))) // 02/11/22 Igor TP#49513
				{
					int submissionMethod;
					char seps[] = ",", codes[31] = { 0 }, *next_token;
					strcpy_s(codes, sizeof(codes), Group_blk.AdditionalMaximumBenefitIndividualStatusCodes);
					char* token = strtok_s(codes, seps, &next_token);
					while (token != NULL)
					{
						if (sscanf_s(token, "%d", &submissionMethod) > 0)
						{
							if (pRX_Message->Status == submissionMethod) // 03/23/18 Igor TP#37951 - match Submission Method
							{
								if (TotalAccumulated > TotalMaxBenefits)
								{
									TotalMaxBenefits += Group_blk.AdditionalMaximumBenefitIndividual;

									AdditionalMaximumBenefitApplied = Group_blk.AdditionalMaximumBenefitIndividual; // 04/09/18 Igor TP#38034 - added AdditionalMaximumBenefitApplied

									if (!pOverTheLimitForReversal) // 07/14/20 Igor TP#44421 - added condition to prevent this on reversal
										reporter.info("Program limit <%.2f> based on %sIndividual Maximum benefit of <%.2f> and Additional amount of <%.2f>",
											TotalMaxBenefits,
											Claimant_blk.AnnualMaxBenefitAddtlAmt ? "Adjusted " : "",
											Group_blk.MaximumBenefitIndividual
											+ Claimant_blk.AnnualMaxBenefitAddtlAmt,
											Group_blk.AdditionalMaximumBenefitIndividual);
								}

								break;
							}
						}
						token = strtok_s(NULL, seps, &next_token);
					}
				}
			}
		}

		// 11/06/19 Igor TP#41895 - moved line here to allow negative amount
		Amount_blk.BenefitRemaining = TotalMaxBenefits - TotalAccumulated;

		if (TotalAccumulated <= TotalMaxBenefits)
		{
			// 12/10/15 Igor TP#30675
			// 11/06/19 Igor TP#41895 - moved line outside if block to allow negative amount
			//Amount_blk.BenefitRemaining = TotalMaxBenefits - TotalAccumulated;

			// sprintf_s(pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), "%.0f", Amount_blk.BenefitRemaining * 100.0);
			CalculateOverPunch(Amount_blk.BenefitRemaining, pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), true); // 02/25/22 Igor TP# 
		}
		else
		{
			if (pOverTheLimitForReversal) // 07/14/20 Igor TP#44421 - added condition
				*pOverTheLimitForReversal = abs(Amount_blk.BenefitRemaining); // 11/06/19 Igor TP#41895
			Amount_blk.BenefitRemaining = 0; // 12/14/15 Igor TP#30675

			PriorAuthorization* pa = UniquePA(pRX_Message->Claim.PAArray, "M"); // 10/28/19 Igor TP#41963 - need PA to skip coupon adjustments 

			if (pa && pa->MaximumBenefit > 0) // 02/18/20 Igor TP#42956 - added MaximumBenefit condition
			{
				reporter.info("PA <%s> type <%s> allows to bypass patient Maximum Benefit Individual rule <%d>", pa->Number, pa->OpusType, Group_blk.MaxBenefitIndividualRule);
			}
			else
			{
				double curcoup = CouponAmt;
				double curPatientPayAmount = PatientPayAmount;		// 02/27/2022 Troy TP#49606
				// 07/18/13 Igor TP#23982 - Rule 3  is based on a calendar year, with the ability to apply a partial benefit
				// 12/05/13 Igor TP#24469 - Rule 4  is based on an year from first fill, with the ability to apply a partial benefit
				// 01/29/14 Igor TP#24844 - Rule 5  is based on m_Claimant.BenefitStartDate, with the ability to apply a partial benefit
				// 03/14/14 Igor TP#25144 - Rule 7  is based on variable Benefit Periods (PatientBenefitPeriods and BenefitRulesAcrossTimePeriod tables), with the ability to apply a partial benefit
				// 09/10/15 Igor TP#29994 - Rule 9  is based on all uses of the card (or cards if linking cards together)
				// 11/02/20 Troy TP#45555 - Rule 10 is based on the CustomMaxBenefitIndividualSP with the ability to apply a partial benefit
				if (Group_blk.MaxBenefitIndividualRule == 3 || Group_blk.MaxBenefitIndividualRule == 4 ||
					Group_blk.MaxBenefitIndividualRule == 5 || Group_blk.MaxBenefitIndividualRule == 7
					|| Group_blk.MaxBenefitIndividualRule == 9  // 09/10/15 Igor TP#29994
					|| Group_blk.MaxBenefitIndividualRule == 10 // 11/02/20 Troy TP#45555
					|| Claimant_blk.AllowPartialBenefit == 1	// 08/20/21 Igor TP#48531
					)
				{
					double diff = TotalAccumulated - TotalMaxBenefits;

					if (diff <= CouponAmt)
					{
						if (Group_blk.DebitRxPlan != 'Y' //on debit PatientPayAmount does not change
							&& Group_blk.DebitRxPlan != 'H') // 05/24/18 Igor TP#38648
							PatientPayAmount += diff;

						CouponAmt -= diff;
					}
					else
					{
						PatientPayAmount += CouponAmt;
						CouponAmt = 0;
					}

					// 04/10/23 Troy
					reporter.info("Partial Benefits Allowed For Annual Cap with Maximum Benefit Individual rule <%d>", Group_blk.MaxBenefitIndividualRule);
				}
				// 07/18/13 Igor TP#23982 - Rule 1  is based on a calendar year, we don't apply a partial benefit
				// 12/05/13 Igor TP#24469 - Rule 2  is based on an year from first fill, we don't apply a partial benefit
				// 01/29/14 Igor TP#24844 - Rule 6  is based on m_Claimant.BenefitStartDate, we don't apply a partial benefit
				// 03/14/14 Igor TP#25144 - Rule 8  is based on variable Benefit Periods (PatientBenefitPeriods and BenefitRulesAcrossTimePeriod tables), we don't apply a partial benefit
				// 11/02/20 Troy TP#45555 - Rule 11 is based on the CustomMaxBenefitIndividualSP, we don't apply a partial benefit
				else
				{
					PatientPayAmount += CouponAmt;
					CouponAmt = 0;

					// 04/10/23 Troy
					reporter.info("Partial Benefits Not Allowed For Annual Cap with Maximum Benefit Individual rule <%d>", Group_blk.MaxBenefitIndividualRule);
				}
				
				CouponAmt = RoundDouble(CouponAmt); // 07/15/16 Igor TP#32480 - round CouponAmt

				// 02/08/22 Igor TP#49513 - new Benefit Exceeded Behavior logic
				char BenefitExceededBehavior = Group_blk.MaxIndividBenefitExceededBehavior;

				if ((Group_blk.BenefitDeterminationMethod & BDM_TIERED) == BDM_TIERED) // 12/09/22 Igor OCLE-4639
					BenefitExceededBehavior = Group_blk.Tier1MaxIndividBenefitExceededBehavior;
				// 02/28/22 Troy TP#49609 - If patient is eligible for expanded benefit, need that to take priority over the maximizer
				else if ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER) // 02/11/22 Igor TP#49513
					BenefitExceededBehavior = Group_blk.MaximizerMaxIndividBenefitExceededBehavior;
				else if (((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED) || EMBPriorAuthUsed == 3) // 04/25/23 Troy OCLE-4874 - added EMBPriorAuthUsed
					BenefitExceededBehavior = Group_blk.ExpandedMaxIndividBenefitExceededBehavior;

				// 12/15/22 Igor OCLE-4639
				long long benefitDeterminationMethod = 0;
				if ((Group_blk.BenefitDeterminationMethod & BDM_TIERED) == BDM_TIERED)
					benefitDeterminationMethod = BDM_TIERED;
				else if ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER)
					benefitDeterminationMethod = BDM_MAXIMIZER;

				// 02/25/22 Troy TP#49255
				//if (CouponAmt <= 0)
				if (CouponAmt <= 0 || (CouponAmt > 0 && BenefitExceededBehavior == 'O'))
				{
					if (BenefitExceededBehavior == 'P' || BenefitExceededBehavior == 'O') // 02/08/22 Igor TP#49513 - added behavioral type 'O'
					{
						CouponAmt = 0;
						Group_blk.BenefitDeterminationMethod |= BDM_EXCEEDED;

						if (BenefitExceededBehavior == 'O') // 02/08/22 Igor TP#49513 - type 'O' allows to pay full amount one time only
						{
							char Date[PACKED_DATE_SIZE] = { 0 };

							int retValue = 0;

							if ((Group_blk.BenefitDeterminationMethod & benefitDeterminationMethod) == benefitDeterminationMethod)
							{
								retValue = GetClaimDateByBenefitDeterminationMethod(Date, sizeof(Date), BDM_EXCEEDED + benefitDeterminationMethod, 0);

								if (retValue == SQL_PROCESSING_ERROR)
								{
									reporter.error("GetClaimDateByBenefitDeterminationMethod returns SQL error");

									Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 306);
									return SQL_PROCESSING_ERROR;
								}
								else if (retValue == RETURN_SUCCESS)
								{
									reporter.warn("%sBenefit Exceeded on <%2.2s/%2.2s/%2.2s>", benefitDeterminationMethod == BDM_TIERED ? "Tier1 " : benefitDeterminationMethod == BDM_MAXIMIZER ? "Maximizer " : "", // 12/15/22 Igor OCLE-4639
										Date + 4, Date + 6, Date + 2);
									
									// 02/05/24 Troy OCLE-5258 - If they already had the one-time exception, that means they would have $0 benefit remaining.
									// AbbVie client team wants to use Qualifier 17 instead of 16 for Tier1.
									Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded,
										benefitDeterminationMethod == BDM_TIERED ? 17 : 13); // 12/09/22 Igor OCLE-4639

									return RETURN_FAILURE;
								}
							}

							retValue = GetClaimDateByBenefitDeterminationMethod(Date, sizeof(Date), BDM_EXCEEDED, benefitDeterminationMethod);

							if (retValue == SQL_PROCESSING_ERROR)
							{
								reporter.error("GetClaimDateByBenefitDeterminationMethod(2) returns SQL error");

								Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 305);
								return SQL_PROCESSING_ERROR;
							}
							else if (retValue == RETURN_SUCCESS)
							{
								reporter.warn("Benefit Exceeded on <%2.2s/%2.2s/%2.2s>",
									Date + 4, Date + 6, Date + 2);
								// 02/25/22 Troy TP#49255 - At this point patient is not maximizer (excluded from the query above), so want to use qualifier 8 instead of 14
								//Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 14);

								// 02/05/24 Troy OCLE-5258 - If they already had the one-time exception, that means they would have $0 benefit remaining.
								// AbbVie client team wants to use Qualifier 12 instead of 8 in that case.
								Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 12);

								return RETURN_FAILURE;
							}
							else
							{
								PatientPayAmount = RoundDouble(curPatientPayAmount);			// 02/27/2022 Troy TP#49606
								CouponAmt = RoundDouble(curcoup);
							}
						}
					}
					else // BenefitExceededBehavior == 0 or 'R' or 'C'
						if (!pOverTheLimitForReversal) // 07/14/20 Igor TP#44421 - added condition to prevent reject on reversal
						{
							reporter.warn("Individual Benefits Used <%.2f>, This <%.2f>, Maximum %s<%.2f> (Reject 76)", 
								AccumulatedBenefitIndividual,
								curcoup, 
								(Group_blk.BenefitDeterminationMethod& BDM_TIERED) == BDM_TIERED ? "(Tiered) " : // 12/09/22 Igor OCLE-4639
								(((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED) || EMBPriorAuthUsed == 3) ? "(Expanded) " :	// 04/25/23 Troy OCLE-4874 - added EMBPriorAuthUsed
								(Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER ? "(Maximizer) " : "", // 02/08/22 Igor TP#49513 - updated wording
								TotalMaxBenefits);

							if (BenefitExceededBehavior == 'C') // 07/30/21 Igor TP#47569
							{
								if (TotalMaxBenefits - AccumulatedBenefitIndividual > 0)
								{
									// 04/07/23 Troy OCLE-4834 - separate the qualifiers based on which annual cap was reached
									//Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 8);
									Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded,
										(Group_blk.BenefitDeterminationMethod & BDM_TIERED) == BDM_TIERED ? 16 :
										(((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED) || EMBPriorAuthUsed == 3) ? 14 :	// 04/25/23 Troy OCLE-4874 - added EMBPriorAuthUsed
										(Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER ? 13 : 8);
								}
								else
								{
									// 04/07/23 Troy OCLE-4834 / OCLE-4841 - separate the qualifiers based on which annual cap was reached
									//Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 12); // 06/01/21 Igor TP#47569 - separate qualifier if available card balance is <= $0
									Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded,
										(Group_blk.BenefitDeterminationMethod & BDM_TIERED) == BDM_TIERED ? 17 :
										(((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED) || EMBPriorAuthUsed == 3) ? 18 :  // 04/25/23 Troy OCLE-4874 - added EMBPriorAuthUsed
										(Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER ? 19 : 12);
								}
							}
							else
							{
								// 02/25/22 Troy TP#49255 - separate qualifier for Expanded and Maximizer related caps
								//Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 8);
								Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 
									(Group_blk.BenefitDeterminationMethod & BDM_TIERED) == BDM_TIERED ? 16 : // 12/09/22 Igor OCLE-4639
									(((Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED) || EMBPriorAuthUsed == 3) ? 14 :	// 04/25/23 Troy OCLE-4874 - added EMBPriorAuthUsed
									(Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER ? 13 : 8);
							}

							// 02/28/22 Igor - the message is coming from CustomClaimResponses now
							// AddResponseMessage(pRX_Message, "INDIVIDUAL MAX.BENEFIT EXCEEDED"); // 03/12/13 Igor

							//sprintf_s(pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), "%.0f", (TotalMaxBenefits - AccumulatedBenefitIndividual) * 100.0);
							CalculateOverPunch(TotalMaxBenefits - AccumulatedBenefitIndividual, pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), true); // 02/25/22 Igor TP# 

							return RETURN_FAILURE;
						}
				}

				if (curcoup != CouponAmt && !pOverTheLimitForReversal) // 07/14/20 Igor TP#44421 - added condition to prevent this on reversal
				{
					reporter.info("Current Benefit of <%.2f> reduced to <%.2f> based on Accumulated <%.2f> and Maximum <%.2f> Individual Benefits",
						curcoup, CouponAmt, AccumulatedBenefitIndividual, TotalMaxBenefits);

					// 11/23/21 Igor TP#49090 - added a custom paid message !PPBAC
					const int maxNumOfMessages = 10;

					char *customMessages[maxNumOfMessages] = { 0 }; // will be used as two-dimensional array

					int custClaimResponses = GetCustomClaimResponses(pRX_Message,
						(Group_blk.BenefitDeterminationMethod & BDM_TIERED) == BDM_TIERED ? "!PPBACT" : // 12/09/22 Igor OCLE-4639
						(Group_blk.BenefitDeterminationMethod & BDM_EXPANDED) == BDM_EXPANDED ? "!PPBACE" :
						(Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER ? "!PPBACM" : "!PPBAC" // 03/02/22 Troy TP#49626 
						, 0, customMessages, maxNumOfMessages, Dispenser_blk.Unique_ID,
						Dispenser_blk.Chain_Code);

					if (custClaimResponses == SQL_PROCESSING_ERROR)
					{
						reporter.error("GetCustomClaimResponses returns SQL Error");

						Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 300);
						return RETURN_FAILURE;
					}
					else if (custClaimResponses)
					{
						for (int num = 0; num < custClaimResponses; num++)
						{
							AddResponseMessage(pRX_Message, customMessages[num]);
							free(customMessages[num]);
						}
					}
				}
			}
		}

		Amount_blk.AccumulatedBenefitPaid = AccumulatedBenefitIndividual; //04/26/18 Igor TP#38114

		// 02/05/24 Troy OCLE-5207
		if ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) == BDM_MAXIMIZER && AccumulatedMaximizerBenefit && AccumulatedBenefitIndividualSave)
		{
			// Maximizer patient where we needed to only consider certain submission methods. Now we also need to ensure that this
			// does not bring them over the Expanded Annual Cap.
			AccumulatedBenefitIndividual = AccumulatedBenefitIndividualSave;
			TotalMaxBenefits = Group_blk.ExpandedMaximumBenefitIndividual + Claimant_blk.AnnualMaxBenefitAddtlAmt;

			if (TotalAdjustmentAmount)
			{
				double current = AccumulatedBenefitIndividual;
				AccumulatedBenefitIndividual -= TotalAdjustmentAmount;
				if (AccumulatedBenefitIndividual < 0)
					AccumulatedBenefitIndividual = 0;
			}

			TotalAccumulated = AccumulatedBenefitIndividual + CouponAmt;

			if (Group_blk.AdditionalMaximumBenefitIndividual)
			{
				if ((Group_blk.BenefitDeterminationMethod & BDM_ADVANCED_REFILL_RULES) == BDM_ADVANCED_REFILL_RULES)
				{
					TotalMaxBenefits += Group_blk.AdditionalMaximumBenefitIndividual;

					AdditionalMaximumBenefitApplied = Group_blk.AdditionalMaximumBenefitIndividual; // 04/09/18 Igor TP#38034 - added AdditionalMaximumBenefitApplied
				}
				else
				{
					if (Group_blk.AdditionalMaximumBenefitIndividualRule == 1
						|| (Group_blk.AdditionalMaximumBenefitIndividualRule == 2
							&& ((Group_blk.BenefitDeterminationMethod & BDM_MAXIMIZER) != BDM_MAXIMIZER)))
					{
						int submissionMethod;
						char seps[] = ",", codes[31] = { 0 }, * next_token;
						strcpy_s(codes, sizeof(codes), Group_blk.AdditionalMaximumBenefitIndividualStatusCodes);
						char* token = strtok_s(codes, seps, &next_token);
						while (token != NULL)
						{
							if (sscanf_s(token, "%d", &submissionMethod) > 0)
							{
								if (pRX_Message->Status == submissionMethod)
								{
									if (TotalAccumulated > TotalMaxBenefits)
									{
										TotalMaxBenefits += Group_blk.AdditionalMaximumBenefitIndividual;

										AdditionalMaximumBenefitApplied = Group_blk.AdditionalMaximumBenefitIndividual;
									}

									break;
								}
							}
							token = strtok_s(NULL, seps, &next_token);
						}
					}
				}
			}

			if (TotalAccumulated > TotalMaxBenefits)
			{


				double curcoup = CouponAmt;
				double curPatientPayAmount = PatientPayAmount;
				// Rule 3  is based on a calendar year, with the ability to apply a partial benefit
				// Rule 4  is based on an year from first fill, with the ability to apply a partial benefit
				// Rule 5  is based on m_Claimant.BenefitStartDate, with the ability to apply a partial benefit
				// Rule 7  is based on variable Benefit Periods (PatientBenefitPeriods and BenefitRulesAcrossTimePeriod tables), with the ability to apply a partial benefit
				// Rule 9  is based on all uses of the card (or cards if linking cards together)
				// Rule 10 is based on the CustomMaxBenefitIndividualSP with the ability to apply a partial benefit
				if (Group_blk.ExpandedMaxBenefitIndividualRule == 3 || Group_blk.ExpandedMaxBenefitIndividualRule == 4
					|| Group_blk.ExpandedMaxBenefitIndividualRule == 5 || Group_blk.ExpandedMaxBenefitIndividualRule == 7
					|| Group_blk.ExpandedMaxBenefitIndividualRule == 9 || Group_blk.ExpandedMaxBenefitIndividualRule == 10
					|| Claimant_blk.AllowPartialBenefit == 1
					)
				{
					double diff = TotalAccumulated - TotalMaxBenefits;

					if (diff <= CouponAmt)
					{
						if (Group_blk.DebitRxPlan != 'Y' //on debit PatientPayAmount does not change
							&& Group_blk.DebitRxPlan != 'H')
							PatientPayAmount += diff;

						CouponAmt -= diff;
					}
					else
					{
						PatientPayAmount += CouponAmt;
						CouponAmt = 0;
					}
				}
				// Rule 1  is based on a calendar year, we don't apply a partial benefit
				// Rule 2  is based on an year from first fill, we don't apply a partial benefit
				// Rule 6  is based on m_Claimant.BenefitStartDate, we don't apply a partial benefit
				// Rule 8  is based on variable Benefit Periods (PatientBenefitPeriods and BenefitRulesAcrossTimePeriod tables), we don't apply a partial benefit
				// Rule 11 is based on the CustomMaxBenefitIndividualSP, we don't apply a partial benefit
				else
				{
					PatientPayAmount += CouponAmt;
					CouponAmt = 0;
				}

				CouponAmt = RoundDouble(CouponAmt);

				char BenefitExceededBehavior = Group_blk.ExpandedMaxIndividBenefitExceededBehavior;

				if (CouponAmt <= 0)
				{
					if (BenefitExceededBehavior == 'P')
					{
						CouponAmt = 0;
						Group_blk.BenefitDeterminationMethod |= BDM_EXCEEDED;

					}
					else // BenefitExceededBehavior == 0 or 'R' or 'C'
					{
						if (!pOverTheLimitForReversal)
						{
							reporter.warn("Individual Benefits Used <%.2f>, This <%.2f>, Maximum Expanded <%.2f> (Reject 76)",
								AccumulatedBenefitIndividual,
								curcoup,
								TotalMaxBenefits);

							if (BenefitExceededBehavior == 'C')
							{
								if (TotalMaxBenefits - AccumulatedBenefitIndividual > 0)
								{
									Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 14);
								}
								else
								{
									Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 18);
								}
							}
							else
							{
								Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 14);
							}

							CalculateOverPunch(TotalMaxBenefits - AccumulatedBenefitIndividual, pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), true);

							return RETURN_FAILURE;
						}
					}
				}
			}
		}

	}

	if (Group_blk.MaximumBenefitFamily > 0.00 && (FamilyCount > 1 || FamilyCount == -1))
	{
		double AccumulatedBenefitFamily = 0;
		char BenefitStartDate[PACKED_DATE_SIZE] = { 0 }, BenefitEndDate[PACKED_DATE_SIZE] = { 0 };

		int retValue = AccumulatedBenefits(AccumulatedBenefitFamily, Claimant_blk, pRX_Message, FALSE, NULL); // family  // 02/05/24 Troy OCLE-5207 - Added NULL for IncludedSubmissionMethods

		if (retValue == SQL_PROCESSING_ERROR) // 09/01/17 Igor TP#36039
		{
			reporter.error("AccumulatedBenefits returns SQL error");

			Send_Reject(pRX_Message, Reject_99_Host_Processing_Error, 186);
			return SQL_PROCESSING_ERROR;
		}

		reporter.info("Accumulated Family Benefit <%.2f>", AccumulatedBenefitFamily);

		if (Claimant_blk.AnnualMaxBenefitAddtlAmt && !pOverTheLimitForReversal) // 07/14/20 Igor TP#44421 - added condition to prevent this on reversal
		{
			reporter.info("Program limit of <%.2f> based on Family Maximum benefit of <%.2f> and Annual Additional amount of <%.2f>",
				Group_blk.MaximumBenefitFamily + Claimant_blk.AnnualMaxBenefitAddtlAmt,
				Group_blk.MaximumBenefitFamily,
				Claimant_blk.AnnualMaxBenefitAddtlAmt);
		}

		if (CouponAmt + AccumulatedBenefitFamily <= (Group_blk.MaximumBenefitFamily + Claimant_blk.AnnualMaxBenefitAddtlAmt))
		{
			AccumulatedBenefitFamily += CouponAmt;

			// 12/10/15 Igor TP#30675
			Amount_blk.BenefitRemaining = (Group_blk.MaximumBenefitFamily + Claimant_blk.AnnualMaxBenefitAddtlAmt) - AccumulatedBenefitFamily;

			//sprintf_s(pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), "%.0f", Amount_blk.BenefitRemaining * 100.0);
			CalculateOverPunch(Amount_blk.BenefitRemaining, pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), true); // 02/25/22 Igor TP# 
		}
		else if (!pOverTheLimitForReversal) // 07/14/20 Igor TP#44421 - added condition to prevent reject on reversal
		{
			reporter.warn("Family Benefits Used <%.2f>, This <%.2f>, Maximum <%.2f> (Reject 76)", AccumulatedBenefitFamily, CouponAmt,
				Group_blk.MaximumBenefitFamily);

			Send_Reject(pRX_Message, Reject_76_Plan_Limitations_Exceeded, 0);
			AddResponseMessage(pRX_Message, "FAMILY MAX.BENEFIT EXCEEDED"); // 03/12/13 Igor

			//sprintf_s(pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), "%.0f", ((Group_blk.MaximumBenefitFamily + Claimant_blk.AnnualMaxBenefitAddtlAmt) - (AccumulatedBenefitFamily)) * 100.0);
			CalculateOverPunch((Group_blk.MaximumBenefitFamily + Claimant_blk.AnnualMaxBenefitAddtlAmt) - (AccumulatedBenefitFamily), pRX_Message->Response.AvailableBenefit, sizeof(pRX_Message->Response.AvailableBenefit), true); // 02/25/22 Igor TP# 

			return RETURN_FAILURE;
		}

		Amount_blk.AccumulatedBenefitPaid = AccumulatedBenefitFamily; //04/26/18 Igor TP#38114
	}

	return RETURN_SUCCESS;
}
