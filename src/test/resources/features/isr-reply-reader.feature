@ixr
Feature: ISR reply file ingestion
  The IXR reader picks up incoming _ISR reply files from the exchange and stores
  one verdict row per transaction block, so downstream collections processing can
  react to each end-to-end payment outcome individually (fan-out per R-17).
  Replaying the same reply file must never duplicate verdicts.

  Scenario: Each transaction block in a reply file becomes its own verdict row
    Given an ISR reply file "20260713_FNB_ISR_reply.xml" answering original message "MSG-1001"
    And the reply reports transaction "E2E-1" with status "ACTC"
    And the reply reports transaction "E2E-2" with status "ACTC"
    And the reply reports transaction "E2E-3" with status "ACTC"
    When the reader job ingests the reply file
    Then the job completes
    And 3 verdict rows are stored for the reply file
    And the verdict for "E2E-2" records status "ACTC" and original message "MSG-1001"

  Scenario: A rejected transaction keeps its reason code
    Given an ISR reply file "20260713_FNB_ISR_rejects.xml" answering original message "MSG-1002"
    And the reply reports transaction "E2E-10" with status "ACTC"
    And the reply reports transaction "E2E-11" with status "RJCT" and reason "AC04"
    When the reader job ingests the reply file
    Then the job completes
    And the verdict for "E2E-11" records status "RJCT" and reason "AC04"
    And the verdict for "E2E-10" records no reason

  Scenario: Replaying the same reply file updates verdicts instead of duplicating them
    Given an ISR reply file "20260713_FNB_ISR_replay.xml" answering original message "MSG-1003"
    And the reply reports transaction "E2E-20" with status "ACTC"
    And the reply reports transaction "E2E-21" with status "ACTC"
    And the reader job has already ingested the reply file
    When the same reply file is ingested again as a new job instance
    Then the job completes
    And 2 verdict rows are stored for the reply file
    And the verdict for "E2E-20" records status "ACTC" and original message "MSG-1003"

  Scenario: A reply file without an original message id fails the job
    Given a malformed ISR reply file "20260713_FNB_ISR_broken.xml" with no original message id
    When the reader job ingests the reply file
    Then the job fails

  Scenario: A reply file with no transactions completes without storing verdicts
    Given an ISR reply file "20260713_FNB_ISR_empty.xml" answering original message "MSG-1004"
    When the reader job ingests the reply file
    Then the job completes
    And 0 verdict rows are stored for the reply file
