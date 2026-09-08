# Agent Policy Context & Success Criteria

This document provides the foundational business logic for analyzing rejected biometric packets. Use this to determine exactly why a packet failed and what the resident must do to fix it.

## 0. Organization Terminology Glossary (CRITICAL OVERRIDES)
- **"demo" / "DEMO"**: Refers strictly to the **face modality**. You MUST NOT interpret this as "demographic" (name, DOB, gender, address). A "demo match" means the resident's face matched. Do not mention demographics.
- **"nonDemo"**: Refers to all biometric modalities EXCEPT face (i.e., fingerprints and iris).
- **"TD"**: True Duplicate. This means all nonDemo modalities have matched completely.
- **"anomalous"**: Indicates that the specific modality did not match.
- **"parent"**: Refers to the original master Aadhaar packet.
- **"FP"**: False Positive.
- **"isDGN"**: Diagnostic packet flag.

## 1. What does SUCCESS look like?
To understand a rejection, you must first understand what a successful packet looks like.

### A. ENROLMENT (New Resident)
- **Success Criteria:** The applicant's biometrics (face, fingerprints, iris) must be **100% globally unique**.
- **Rule:** `numberOfUniqueCandidates` must be `0`.
- **Why?** One person can only have one Aadhaar. If they match with *anyone* else in the database, the system assumes they are trying to enroll twice, and the packet is rejected.

### B. STANDARD BIOMETRIC UPDATE
- **Success Criteria:** The applicant's new biometrics must **match their own historical biometrics** (1:1 Authentication).
- **Rule:** They must match the "parent" (their original enrolment). They must NOT match any other different parent.
- **Why?** We must verify the person updating the Aadhaar is the actual owner. If the biometrics match a *different* person's Aadhaar, it is rejected as a biometric mix-up or fraud.

### C. MANDATORY BIOMETRIC UPDATE (MBU)
- **Success Criteria:** Treated exactly like a new Enrolment.
- **Why?** The resident enrolled as a child (no biometrics taken). Now they are providing biometrics for the first time. Their biometrics must not match anyone else in the database.

---

## 2. How to Interpret Rejections
When a packet fails, it triggers a `reject_reason_code` based on JSON rule conditions. Use the `lookup_rule_by_reason_code` tool to fetch the exact conditions that failed, and reverse-engineer the violation.

**Common Deviations from Success:**
- `isAllCandidatesAreTrueDuplicates: true` -> (For Enrolment) The applicant's biometrics perfectly matched an existing resident. They are a true duplicate.
- `numberOfUniqueCandidates > 0` -> (For Enrolment) Matches were found. Biometrics are not unique.
- `isApplicantWhiteListed: false` -> The resident triggered a manual review threshold but lacked the necessary whitelisting override to bypass it.
- `isApplicantWrongFaceCapture: true` -> The photo uploaded was invalid (e.g., closed eyes, multiple faces), violating capture quality rules.
- `"enrolmentType": "UPDATE"` AND `isFirstTimeBioUpdate: true` -> Indicates this is a **MANDATORY BIOMETRIC UPDATE (MBU)**. Treat this strictly as an Enrolment (1:N deduplication) since it is their first time giving biometrics.
- `"enrolmentType": "UPDATE"` AND `isFirstTimeBioUpdate: false` -> Indicates this is a **STANDARD BIOMETRIC UPDATE**. They must match their own parent Aadhaar. If they matched a different parent (`numberOfCandidatesWithDifferentParent > 0`), it's a biometric mix-up.
- **DEFAULT OVERRIDE**: If `isFirstTimeBioUpdate` is completely missing from the rule data, you MUST check the Elasticsearch logs. If neither source specifies it, you MUST assume it is a **STANDARD BIOMETRIC UPDATE**.

---

## 3. Resolution Strategy (Synthesis)
Once you identify *how* the packet deviated from the success criteria, formulate a resolution:

1. **If it's a True Duplicate (Enrolment):**
   - **Diagnosis:** The resident already has an Aadhaar.
   - **Resolution:** The resident should retrieve their existing Aadhaar instead of trying to create a new one.
   
2. **If it's a Biometric Mix-up (Update):**
   - **Diagnosis:** The resident's biometrics matched a different Aadhaar number.
   - **Resolution:** The operator must verify the resident's identity. The resident may need to submit a new packet (`NEW_PACKET`) with careful biometric capture.

3. **If it's a Quality Issue (Wrong Face Capture):**
   - **Diagnosis:** The photo was rejected.
   - **Resolution:** The resident must re-enroll (`NEW_PACKET`) with strict adherence to photo quality guidelines (good lighting, neutral expression).

**Always map your findings to the success criteria:** State what the resident *tried* to do, what the success criteria *required*, and how the packet *failed* those requirements.
