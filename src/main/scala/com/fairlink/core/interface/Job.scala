package com.fairlink.core.interface

import com.fairlink.infrastructure.interface.{TableReader, TableWriter}

trait Job {
  protected val reader: TableReader
  protected val processor: Processor
  protected val writer: TableWriter
  def run(): Unit
}
