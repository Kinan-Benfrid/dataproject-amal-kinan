package Configuration

import java.sql.Timestamp

case class Client(
                   IdentifiantClient: Long,
                   Nom: String,
                   Prenom: String,
                   Adresse: String,
                   DateDeSouscription: Timestamp)
