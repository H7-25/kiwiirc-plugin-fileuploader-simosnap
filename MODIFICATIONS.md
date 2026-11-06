# Provenienza e modifiche

Versione indipendente SimosNap derivata dal branch simosnap di H7-25/kiwiirc-plugin-fileuploader al commit 62c73be6f54cb5787fde806b95e9b671e090d919, a sua volta derivato dal progetto KiwiIRC FileUploader. Licenze e attribuzioni sono conservate, incluso Copyright 2018 ItsOnlyBinary nella licenza frontend.

Il modulo Go conserva il proprio percorso originario per non riscrivere gli import della versione verificata. Il nuovo repository non dipende dalla relazione GitHub di fork.

## File modificati rispetto alla base

Le modifiche comprendono gli stati degli upload, recovery, lock, letture concorrenti, migrazioni, verificatore SQL, frontend retry e relativi test. Il rapporto fase 2 descrive le responsabilita. Sono incluse anche le due correzioni ESLint del gestore Promise.

- `config/config.go`
- `config/default-config.go`
- `db/db.go`
- `expirer/expirer.go`
- `fileuploader-kiwiirc-plugin/package.json`
- `fileuploader-kiwiirc-plugin/src/handlers/uppy/share-completed-upload-url.js`
- `fileuploader.config.example.toml`
- `server/http-range-parser.go`
- `server/tusd-extracts.go`
- `server/tushandlers.go`
- `server/uploadserver.go`
- `shardedfilestore/schema.go`
- `shardedfilestore/shardedfilestore.go`

## File introdotti durante lo sviluppo

- `cmd/verify-migration/main.go`
- `doc/MIGRATION-QUALIFICATION.md`
- `doc/PHASE2-REPORT.md`
- `doc/UPLOAD-LIFECYCLE.md`
- `expirer/expirer_test.go`
- `fileuploader-kiwiirc-plugin/tests/share-url.test.cjs`
- `internal/migrationcheck/check.go`
- `internal/migrationcheck/check_test.go`
- `server/headfile.go`
- `server/read_regression_test.go`
- `server/upload_lifecycle_test.go`
- `shardedfilestore/lifecycle.go`
- `shardedfilestore/lifecycle_test.go`
- `shardedfilestore/locks.go`
- `shardedfilestore/maintenance.go`
- `shardedfilestore/phase2_test.go`
- `shardedfilestore/read.go`

## Adattamenti per questo repository indipendente

README aggiornato; istruzioni upstream conservate in doc/UPSTREAM-README.md; rapporto di qualificazione aggiornato; esempio del fallback EXTJWT privo di segreti; esclusione dei dati runtime; token di esempio rimossi dalla documentazione logging. I default runtime, TTL e autorizzazioni DELETE non sono stati modificati da questa preparazione.
