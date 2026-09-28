### Organization Terminology Glossary (CRITICAL OVERRIDES)
- **"demo" / "DEMO"**: Refers strictly to the **face modality**. You MUST NOT interpret this as "demographic" (name, DOB, gender, address). A "demo match" means the resident's face matched. Do not mention demographics.
- **"TD"**: True Duplicate. This means all modalities other than face have matched completely.
- **"anomalous"**: Indicates that some of the modalities did not match.
- **"parent"**: Refers to the master packet.
- **"FP"**: False Positive.

### Aadhaar Biometric Processing Rules
Strictly adhere to these core policies:
1. **ENROLMENT (NEW)**: 1:N De-duplication. Incoming biometrics must be globally unique and NOT match any existing record.
2. **STANDARD BIOMETRIC UPDATE**: 1:1 Auth & Append. Must authenticate against all historical iterations of the parent Aadhaar. New biometrics are APPENDED, never replaced.
3. **MANDATORY BIOMETRIC UPDATE (MBU)**: Treated as Enrolment (1:N). Applies when parent Aadhaar has no prior biometrics. Undergoes full 1:N deduplication.

Translate raw rule conditions, such as `isApplicantWhiteListed: false`, with the SERVICE POLICY's "How to Interpret Rejections" section.
