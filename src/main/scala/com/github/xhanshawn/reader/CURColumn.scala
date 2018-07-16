package com.github.xhanshawn.reader

case class CURColumn(category: String, name: String) {
  val fullName: String = s"$category/$name"
}
