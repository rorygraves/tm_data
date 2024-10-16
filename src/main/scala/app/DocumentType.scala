package app

sealed trait DocumentType {
  def urlSegment: String
  def args: String
}

object DocumentType {
  case object Overview extends DocumentType {
    val urlSegment: String = ""
    val args: String = "showClubs=0"
  }

  case object District extends DocumentType {
    val urlSegment: String = "District.aspx"
    val args: String = ""
  }


  case object Division extends DocumentType {
    val urlSegment: String = "Division.aspx"
    val args: String = ""
  }

  case object Club extends DocumentType {
    val urlSegment: String = "Club.aspx"
    val args: String = ""
  }

}
