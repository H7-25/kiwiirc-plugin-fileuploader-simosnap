# Qualificazione SimosNap — 6 novembre 2026

Questo rapporto aggiorna i documenti storici delle fasi di sviluppo. I risultati Linux, staging, browser e produzione qui riportati sono stati comunicati dall'operatore durante la qualificazione; non derivano da accesso autonomo dell'agente ai server.

## Evidenze

- Go test, race detector e vet superati su Linux amd64 con Go 1.22.2 e GCC 12.2.0.
- Migrazioni 6 e 7 qualificate su una copia reale coerente di 4.445.678 record: integrita superata, conteggi e impronta degli otto campi preesistenti preservati. Migrazione circa 1,23 secondi; intera verifica circa 2 minuti e 17 secondi; query principali indicizzate.
- Frontend: 13 test JavaScript superati e build con ESLint riuscita. Due correzioni sintattiche al gestore Promise hanno rimosso errori di lint senza cambiare intenzionalmente il comportamento.
- Staging: download/HEAD concorrenti, deduplicazione con revoca di un riferimento, DELETE durante un download aperto da 16 MiB, interruzione PATCH, ripresa dopo shutdown ordinato e SIGKILL dopo un chunk confermato, otto upload concorrenti con contenuti verificati.
- Trasformazione EXIF: offset TUS riferito ai byte originali, GET/Range riferiti ai byte trasformati, contenuto e decodifica verificati.
- Browser: immagini, audio, video, AudioMsgButton, condivisione automatica, upload simultanei e ripresa dopo rete interrotta verificati dall'operatore. Vecchio frontend provato con il nuovo backend. I viewer PHP non fanno parte della prova del backend diretto.
- La configurazione Uppy osservata non abilita concatenazione. Nel browser esaminato non erano salvate sessioni parallele; non e una verifica di tutti gli utenti storici.

## Conflitti con blob preesistenti

Dopo il rilascio alcuni upload sono rimasti in finalizing con `existing blob does not match its hash`. Nel caso esaminato il temporaneo aveva l'hash atteso, il blob definitivo no; l'ultima modifica osservata del blob era precedente al rilascio. Non e stata determinata la causa originaria delle modifiche dei blob. Sono stati osservati anche DELETE 500 su upload coinvolti.

Il controllo impedisce di eliminare il temporaneo valido a favore del blob incoerente, ma non risolve automaticamente il conflitto. Il recovery puo continuare a ritentare. Questa limitazione rimane aperta nel codice pubblicato.

L'operatore ha scelto di archiviare i dati precedenti e ripartire con database e storage vuoti, anziche recuperare i singoli casi. Dopo il reset sono stati osservati migliaia di upload completati e nessun finalizing pendente nelle fotografie successive. Cio non dimostra l'eliminazione della causa originaria.

## TTL degli utenti identificati

E stato riprodotto un upload da sessione identificata con token avente issuer IRCd non presente nella mappa delle chiavi. La configurazione aveva solo un issuer locale e nessun fallback; il backend assegnava il TTL ospiti. La logica JWT non era cambiata nel refactoring.

Con segreto EXTJWT condiviso tra IRCd, e stato aggiunto il fallback `*` nella sola configurazione di produzione. Un nuovo upload ha registrato account e issuer corretti, con scadenza a 24 ore dal completamento. Nessun segreto di produzione e incluso nel repository. Il README documenta il fallback senza alterare i default upstream o le scadenze gia assegnate.

## Limiti della qualificazione

Non sono conclusi: fault injection nei punti di pubblicazione/cancellazione, power loss, misura sostenuta della contesa e delle latenze rispetto a una baseline, tutti i browser e le sessioni storiche, MySQL e filesystem remoti. I conteggi di record deleted da soli non dimostrano la cancellazione fisica di ogni blob. Non assumere un rollback sicuro limitato al vecchio eseguibile.

## Identita degli artefatti verificati

Backend Linux comunicato dall'operatore:
`13092c329f8f1713597b35b47553b23308d3dbb4e6d1892ef1f4789d9b2e3e1e`.

Bundle frontend compilato localmente:
`5af7ef5d1e5b994b8eb77a319cdcca051d7782f6e0007e94339b9571bad56738`.

Gli hash identificano quegli artefatti, non garantiscono una ricompilazione identica con strumenti differenti. Nessun binario o bundle generato viene committato nel repository sorgente.
