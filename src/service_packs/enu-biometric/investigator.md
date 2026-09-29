### Enrolment types for this service
- **N (New Enrolment)**: The packet is a new enrolment. Biometric processing follows 1:N de-duplication rules. Incoming biometrics must be globally unique.
- **U (Biometric Update)**: The packet is a biometric update to an existing Aadhaar. The new biometrics undergo 1:N de-duplication, and the update succeeds only if the matches returned are all historical biometrics of the resident's own parent Aadhaar. This is not a 1:1 authentication. New biometrics are APPENDED to the existing record, never replaced.
- **Z (Reactivation)**: Follows exactly the same rules as U.

### Aadhaar Biometric Processing Rules
Strictly adhere to these core policies:
1. **ENROLMENT (NEW)**: 1:N De-duplication. Incoming biometrics must be globally unique and NOT match any existing record.
2. **STANDARD BIOMETRIC UPDATE**: 1:N De-duplication & Append -- NOT 1:1 authentication. The 1:N result must contain only the historical biometrics of the resident's own parent Aadhaar: no match at all, or any match from a different parent, is a failure. New biometrics are APPENDED, never replaced.
3. **MANDATORY BIOMETRIC UPDATE (MBU)**: Treated as Enrolment (1:N). Applies when parent Aadhaar has no prior biometrics. Undergoes full 1:N deduplication.

### Modality Terminology -- BINDING
- `demo` = the **face** modality only. `nonDemo` = every other biometric modality (fingerprints and iris). nonDemo modalities ARE biometric -- never call them "non-biometric"; write "nonDemo biometric" or "non-face biometric".
- `TD` (True Duplicate) = **all** nonDemo modalities matched completely. Write "fingerprints **and** iris matched" -- never "and/or".
- `DemoTD` = face matched **and** all nonDemo matched: a complete biometric match across every modality, not a face-only match.

### Packet-specific facts to look for
For this service, the packet-specific facts in the logs and tool results are, for example, which candidates matched, whether they share this packet's parent, which modality matched, the scores, and when.
