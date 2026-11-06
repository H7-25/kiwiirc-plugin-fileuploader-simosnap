# KiwiIRC FileUploader — SimosNap

Repository indipendente del backend Go e del plugin KiwiIRC/Uppy mantenuti per SimosNap. Include AudioMsgButton, upload TUS riprendibili, deduplicazione, scadenze e gestione dei riferimenti ai blob condivisi.

Deriva da [kiwiirc/plugin-fileuploader](https://github.com/kiwiirc/plugin-fileuploader) attraverso [H7-25/kiwiirc-plugin-fileuploader](https://github.com/H7-25/kiwiirc-plugin-fileuploader), branch `simosnap`, commit `62c73be6f54cb5787fde806b95e9b671e090d919`. Licenze e attribuzioni originali sono conservate. La nuova storia Git parte dalla versione integrata; non importa la cronologia del repository precedente.

## Backend

Ambiente verificato: Linux amd64, Go 1.22.2, GCC 12.2.0, SQLite incorporato nel driver Go. CGO deve essere abilitato.

```sh
git clone https://github.com/H7-25/kiwiirc-plugin-fileuploader-simosnap.git
cd kiwiirc-plugin-fileuploader-simosnap
GOTOOLCHAIN=local CGO_ENABLED=1 go test -mod=readonly ./...
GOTOOLCHAIN=local CGO_ENABLED=1 go test -mod=readonly -race ./...
GOTOOLCHAIN=local CGO_ENABLED=1 go vet -mod=readonly ./...
mkdir -p build-output
GOTOOLCHAIN=local CGO_ENABLED=1 go build -mod=readonly -o build-output/plugin-fileuploader .
```

Copiare `fileuploader.config.example.toml` in una configurazione locale e adattarla prima dell'avvio:

```sh
./build-output/plugin-fileuploader --config=/percorso/configurazione.toml
```

Lo schema viene migrato automaticamente all'avvio; il maintenance worker puo modificare database e storage. Non avviare il server su dati reali per una semplice verifica. Usare il [verificatore di migrazione](doc/MIGRATION-QUALIFICATION.md) sulle copie.

## Frontend

Build verificata con Node 18.16.1 e Yarn Classic 1.22.22. Queste versioni descrivono la verifica effettuata, non una politica di aggiornamento degli strumenti.

```sh
cd fileuploader-kiwiirc-plugin
yarn install --frozen-lockfile --non-interactive
yarn test
yarn build
```

Pubblicare il contenuto di `fileuploader-kiwiirc-plugin/dist/`, incluse le traduzioni. Il file principale e `plugin-fileuploader.js`; puo essere rinominato secondo la configurazione KiwiIRC, mantenendo coerenti anche gli eventuali `.gz` e `.br`. Caricare un solo uploader per pagina. Non pubblicare la directory dei sorgenti come asset web.

La configurazione corrente del plugin usa `settings["plugin-fileuploader"]`; il nome storico `settings.fileuploader` e gestito come compatibilita. L'endpoint deve essere quello del proprio backend. Il frontend SimosNap riscrive gli URL condivisi sotto `/files/`: i viewer PHP e le relative rotte Nginx appartengono al sito e non sono inclusi in questo repository.

## TTL e autenticazione EXTJWT

I valori predefiniti upstream restano invariati. Il profilo operativo SimosNap configura esplicitamente:

```toml
[Expiration]
MaxAge = "1h"
IdentifiedMaxAge = "24h"
CheckInterval = "5m"
```

Le 24 ore richiedono un token EXTJWT valido con claim `account` non vuoto. Il solo nickname registrato non basta. Se gli IRCd condividono lo stesso segreto di firma, il backend supporta un fallback per issuer diversi:

```toml
[JwtSecretsByIssuer]
"*" = "REPLACE_WITH_YOUR_SHARED_EXTJWT_SECRET"
```

Sostituire il segnaposto nel solo file locale; non committare segreti. Con segreti diversi usare una voce per issuer. Un token assente o rifiutato viene trattato come ospite quando `RequireJwtAccount=false`. La correzione della configurazione non prolunga retroattivamente upload gia creati.

## Protocollo e limiti noti

- Chunk frontend predefiniti di 512 KiB; configurazione osservata senza upload paralleli dello stesso file.
- TUS concatenation non disponibile: POST con `Upload-Concat` restituisce 501. Restano creation, creation-with-upload, termination e creation-defer-length. Non abilitare `parallelUploads > 1` senza rivalutare questa limitazione.
- I download gia aperti possono completarsi dopo DELETE; i nuovi accessi sono revocati.
- Un blob esistente con hash errato blocca la pubblicazione in `finalizing`. Il recovery ritenta ma non ripara automaticamente il conflitto; anche DELETE puo fallire nel percorso osservato. Non esiste una pulizia automatica degli orfani.
- Una scadenza diventa revoca effettiva quando viene elaborata dal worker; l'accesso non viene negato al solo superamento del timestamp.
- La logica JWT corrente puo scrivere il token completo negli avvisi: trattare i log come dati riservati.
- Non e qualificato un rollout con vecchie e nuove istanze sullo stesso storage. Il rollback del solo binario non e un piano di ripristino completo.

## Documentazione

- [Qualificazione e osservazioni operative](doc/SIMOSNAP-QUALIFICATION.md)
- [Ciclo di vita degli upload](doc/UPLOAD-LIFECYCLE.md)
- [Qualificazione delle migrazioni](doc/MIGRATION-QUALIFICATION.md)
- [Rapporto storico della fase 2](doc/PHASE2-REPORT.md)
- [Provenienza e modifiche](MODIFICATIONS.md)

Questo repository conserva la versione verificata e distribuita; i test riportati non equivalgono a garanzia di assenza di difetti. Il database MySQL, i filesystem remoti, i crash durante pubblicazione/cancellazione e la perdita di alimentazione richiedono ulteriori verifiche.

## Licenza

Apache-2.0. Vedere [LICENSE](LICENSE) e la [licenza frontend](fileuploader-kiwiirc-plugin/LICENCE).
