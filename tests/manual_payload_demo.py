"""Manual demo: parse a real rejection payload and print what was extracted.

    python3 tests/manual_payload_demo.py

Renamed off the `test_*.py` pattern. As `test_payload.py` at the repo root it
matched pytest discovery and ran its module-level code on collection, while
never asserting anything -- so it looked like a test and tested nothing
(G22b).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.schemas import MessagePayload

reject_json = """{
	"eventId": "0d6254d3-b951-46eb-8662-5162c9111981",
	"category": "",
	"eventType": "",
	"eventTimestamp": "2026-07-08 10:01:24.231",
	"eventVersion": null,
	"sid": "0872051100469320260703164959",
	"sidDate": "2026-07-03 16:49:59",
	"version": "",
	"sourceTopic": "ENU.BIO.PROCESS.COMPLETION.V2",
	"callbackTopic": "ENU.BIO.PROCESS.COMPLETION.V2",
	"flowMetaData": {
		"stage": "Biometric",
		"subStage": "MDD_POLICY_BATCH_1"
	},
	"taskMetaData": null,
	"packetMetaData": {
		"refId": "0d6254d3-b951-46eb-8662-5162c9111981"
	},
	"packetExecutionSummary": {
		"hasExecutionErrors": false,
		"hasValidationErrors": false,
		"packetStatus": "REJECTED",
		"errorData": [
			{
				"type": null,
				"errorReasonCode": "RESIDENT_MAN_DEDUP_REJECT_TD"
			}
		],
		"isExecutionSuccess": true,
		"isValidationSuccess": true
	},
	"rejectBits": null,
	"requestStage": "ExtIntService",
	"requestStageStatus": null,
	"resubmissionSummary": {
		"resubmissionCount": 2,
		"resubmissionReason": "REPLAY IN-FLIGHT"
	},
	"uidV2DataArray": null
}"""

payload = MessagePayload.model_validate_json(reject_json)
print(f"Parsed Event ID: {payload.eventId}")
print(f"Parsed Status: {payload.packetExecutionSummary.packetStatus}")
print(f"Parsed Errors: {payload.packetExecutionSummary.errorData}")
