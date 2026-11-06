> Documento storico della fase precedente alla qualificazione reale. Per lo stato successivo vedere [SIMOSNAP-QUALIFICATION.md](SIMOSNAP-QUALIFICATION.md).

# Seconda fase — rapporto di sviluppo e verifica locale

Base: branch simosnap, commit 62c73be6f54cb5787fde806b95e9b671e090d919.
Le modifiche della prima e seconda fase sono ancora locali, senza commit, push o
deploy. Nessun database o file di produzione è stato aperto. Il rapporto riguarda
la versione integrata; non costituisce autorizzazione né certificazione del deploy.

## 1. Problemi corretti

- **B1:** GET apre il file sotto lock condiviso e lo rilascia prima della
  trasmissione. HEAD usa lock condivisi e conserva la gestione TUS della
  libreria. Download/HEAD paralleli non si escludono. DELETE può revocare i nuovi
  accessi lasciando terminare i descrittori già aperti. I lock sono distinti per
  chiave completa, con directory suddivise per prefisso, senza slot condivisi.
- **B2:** il recupero dei metadati nel frontend ha tentativi/timeout limitati. Un
  errore temporaneo non perde la condivisione dell'URL; dopo i tentativi viene
  condiviso una volta senza metadati opzionali. Rifiuti permanenti e HEAD 412
  conservano il comportamento precedente. Il timeout funziona anche senza
  AbortController o con fetch che ignora l'abort.
- **B3:** recovery e scadenze usano lotti limitati, cursori separati, priorità
  alternata, budget configurabile e cancellazione. Gli elementi occupati non
  bloccano gli altri. Il recovery dei file non precede più l'apertura del
  listener. I lotti pieni continuano con un ritardo breve per smaltire arretrati.
- **B4/I1:** nessun UPDATE globale per classificare lo storico. Gli stati legacy
  sono dedotti in lettura. Indici parziali SQLite sui record attivi; query
  separate e paginate per scadenze/incompleti; EXISTS per gli altri riferimenti.
  È coperto anche l'upgrade da uno schema 6 della prima fase.
- **I4:** offset/dimensione TUS rimangono quelli originali del trasferimento; GET
  e Range usano la dimensione fisica pubblicata. Sono coperti trasformazioni,
  ultimo chunk interrotto, recovery dopo pubblicazione e ripresa allo stesso URL.
  Una pubblicazione ancora pendente restituisce 423 sul probe di ripresa, senza
  indurre il client a ricreare inutilmente l'upload.
- **I5:** cleanup riuscito non impacchetta inutilmente gli errori HTTP; la risposta
  GET/HEAD riconosce anche errori HTTP contenuti. Il rifiuto 406 degli upload
  vuoti rimane 406.
- **Range preesistenti:** seek sul byte corretto, 416 per intervalli invalidi o
  vuoti, nessun accesso a lista vuota, chiusura del file nei ritorni anticipati.
  La risposta intera è limitata ai byte osservati all'apertura, anche se un
  upload incompleto riceve altre append durante il download.
- **SQLite:** il read-lock copre realmente query e Scan. Le attese dei mutex e
  le operazioni SQL di manutenzione sono cancellabili allo shutdown.

TTL/CheckInterval e corpo delle autorizzazioni DELETE sono stati confrontati
con l'originale e risultano invariati. AudioMsgButton è identico all'originale.
Non è stata eseguita alcuna pulizia degli orfani né disabilitata un'altra
estensione TUS. La concatenazione resta nello stato della prima fase.

## 2. File modificati

Elenco cumulativo rispetto all'originale, inclusi i file nuovi della prima fase
aggiornati nella seconda.

| File | Responsabilità |
|---|---|
| config/config.go | Parametri e validazione dei limiti di manutenzione |
| config/default-config.go | Default manutenzione; nessuna variazione dei TTL |
| fileuploader.config.example.toml | Configurazione documentata della manutenzione |
| db/db.go | Query/Scan protetti insieme, operazioni e attese cancellabili |
| expirer/expirer.go | Cicli limitati, gestione backlog e shutdown |
| shardedfilestore/shardedfilestore.go | Integrazione stati, scrittura, dimensioni logiche, cleanup/errori |
| shardedfilestore/lifecycle.go | Pubblicazione/cancellazione journaled, riferimenti e stati legacy |
| shardedfilestore/locks.go | Lock per chiave, condivisi/esclusivi, attese limitate |
| shardedfilestore/read.go | Apertura download protetta, snapshot di lettura e recovery su accesso |
| shardedfilestore/maintenance.go | Lotti indicizzati, cursori, fairness e cancellazione |
| shardedfilestore/schema.go | Migrazioni 6/7 e API di sola migrazione per la verifica |
| server/headfile.go | HEAD TUS con adapter di sola lettura e lock condiviso |
| server/tusd-extracts.go | Download senza lock durante lo streaming, Range ed errori HTTP |
| server/http-range-parser.go | Rifiuto di suffix range non validi |
| server/tushandlers.go | Collegamento HEAD; rifiuto concatenazione già introdotto nella fase uno |
| server/uploadserver.go | Avvio worker con opzioni e senza recovery sincrono dei file |
| fileuploader-kiwiirc-plugin/src/handlers/uppy/share-completed-upload-url.js | Retry/fallback dei metadati senza perdere il messaggio |
| fileuploader-kiwiirc-plugin/package.json | Comando dei test frontend, nessuna nuova dipendenza |
| cmd/verify-migration/main.go | CLI esplicita di qualificazione su nuova copia |
| internal/migrationcheck/check.go | Backup read-only, conservazione campi, integrità e piani query |
| shardedfilestore/lifecycle_test.go | Suite della prima fase e aggiornamento fixture migrazione |
| shardedfilestore/phase2_test.go | Contesa, lotti, fairness, indici e compatibilità legacy/schema 6 |
| server/upload_lifecycle_test.go | Upload/HEAD/recovery TUS e fixture condivisa |
| server/read_regression_test.go | Letture simultanee, revoca, trasformazioni, Range e protocollo |
| expirer/expirer_test.go | Cancellazione attese DB e smaltimento backlog prima del tick regolare |
| internal/migrationcheck/check_test.go | Protezione sorgente/destinazione e prova opt-in da un milione di righe |
| fileuploader-kiwiirc-plugin/tests/share-url.test.cjs | Test del modulo frontend reale con risposte simulate |
| doc/UPLOAD-LIFECYCLE.md | Contratti, operatività e limiti aggiornati |
| doc/MIGRATION-QUALIFICATION.md | Procedura ripetibile per una snapshot reale |
| doc/PHASE2-REPORT.md | Questo rapporto |

## 3. Test aggiunti ed esiti

La suite contiene 41 test Go di primo livello: 40 ordinari e uno opt-in da un
milione di righe, eseguito separatamente. Rispetto alla prima fase sono stati
aggiunti 21 test Go di primo livello e 13 test frontend. Le sottoprove coprono
anche diversi Range, trasformazioni e creazione di upload vuoti.

Esiti locali:

- go test ./...: superato (il test grande è opt-in).
- go test -race ./...: superato, nessuna race rilevata nei percorsi esercitati.
- go vet ./...: superato.
- Test frontend del modulo effettivo: 13/13 superati, su Node 18.16.1.
- Test su 1.000.000 righe: superato con SQLite 3.46.1 del driver del progetto.
- git diff --check: superato.

I test coprono download lento con altri 24 GET/HEAD, revoca durante trasferimento,
nuovi accessi rifiutati e trasferimento già aperto integro; vecchia collisione di
slot; recovery con blob occupato; cancellazione prima della pubblicazione;
limiti/cursori e alternanza anche con BatchLimit=1; query con ricerca indicizzata;
migrazione senza riscritture dello storico; upgrade dalla prima fase; offset
logici e contenuti trasformati; ultimo chunk interrotto; Range esatti/invalidi e
assenza di leak dei descrittori; deferred length, creation-with-upload e URL con
filename; 406; shutdown e backlog; retry/fallback frontend, audio tag, rifiuti
permanenti, errori di rete e timeout.

La prova grande preserva il conteggio e un digest degli otto campi preesistenti,
richiede integrity_check=ok e ricerche indicizzate senza sort temporaneo nelle
quattro forme di query verificate. Fixture: 95% tombstones, 5% record attivi, dati
sintetici su disco locale. Migrazione circa 0,28 s, backup circa 0,65 s nel run
registrato; non sono stime per il server reale.

Il linker macOS/Go 1.22.1 emette avvisi LC_DYSYMTAB durante i build race; i test
terminano con exit code 0. I test frontend usano VM modules sperimentali solo nel
runner di test. Non è stato eseguito un build completo del bundle né un test in
un browser con un server KiwiIRC reale.

## 4. Problemi ancora aperti

- Durabilità dopo perdita di alimentazione: sincronizzazione delle directory,
  scritture dei comandi esterni e comportamento dello storage da qualificare.
- Comandi pre-finish non idempotenti possono essere rieseguiti prima che venga
  registrato l'intento di finalizzazione; non c'è garanzia exactly-once.
- fsync per chunk e verifica completa dei duplicati richiedono prove di carico.
- I lock persistenti per chiave consumano inodes: pianificare capacità e crescita;
  non rimuoverli mentre processi possono detenerli.
- Le chiamate kernel/filesystem permanentemente bloccate non sono interrompibili
  dal solo context. Il budget limita il lavoro e le attese applicative normali.
- Un budget inferiore al tempo di hash del file massimo può far scadere sempre
  quel tentativo. Il parametro è configurabile, l'esaurimento viene segnalato e
  va dimensionato con prove sullo storage reale.
- Lo swap/drain del server incorporato, la policy di accesso oltre TTL e gli altri
  problemi originali fuori dalle priorità richieste restano invariati.

## 5. Incompatibilità residue

- Tutti i vecchi processi devono essere fermati prima di usare i nuovi lock.
- Concatenazione ancora non disponibile: verificare tusOptions, override per file
  e riprese parallele salvate nei browser. Non sono state disabilitate altre
  funzionalità TUS nella seconda fase.
- In fallback, la condivisione mantiene URL/tipo/dimensione ma può mancare il tag
  expires: il countdown non è garantito senza metadati. Il messaggio non si perde.
- L'implementazione dei lock/download richiede semantiche Unix flock/unlink.
- Migrazioni MySQL non qualificate su un server effettivo.

## 6. Rischi e condizioni prima del deploy

Le correzioni sono una candidata alla validazione pre-rilascio locale, non una
certificazione di idoneità alla produzione. Servono ancora:

1. Procedura della snapshot reale eseguita su una nuova copia, con conservazione
   del report, misure dello spazio di picco e finestra di migrazione.
2. Carico e fault injection su Linux/storage rappresentativi, incluse distribuzioni
   reali dei duplicati, trasformazioni massime e backlog.
3. Verifica browser con KiwiIRC/Uppy effettivi, audio, pause/riprese, filename URL e
   configurazione reale dei proxy e delle eventuali opzioni parallele.
4. Piano di stop coordinato, backup DB/file coerenti e rollback delle migrazioni.

Nessuna di queste attività è stata eseguita su dati o servizi di produzione.
