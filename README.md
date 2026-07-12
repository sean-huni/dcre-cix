# dcre-ixr

ISR response-leg reader (SCRUM-25, M4). Ingests synthetic pain.002-family ISR reply files (token _ISR) into isr_resp: one row per Tx block, fan-out at ingest per R-17. Replay-safe via INSERT ... ON CONFLICT (response_file, e2e). 3-tier: ReaderTasklet -> ReaderService -> data/repo. [SYNTHETIC-CONTRACT R-35] reply shape.
