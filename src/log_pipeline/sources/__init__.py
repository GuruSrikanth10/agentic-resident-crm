"""Log sources -- Stage 1 of the reduction pipeline.

Each source implements the `LogSource` protocol and returns canonical
`LogRecord`s, so Stages 2-4 remain source-agnostic.
"""
