"""Background workers for the kernel.

Currently empty: the embedding generator/inserter live in orchestra-platform
because they depend on OpenAI client setup and the platform's batched
embedding helper. Phase-2 work will extract a kernel-friendly OpenAI
embedding helper into orchestra-core so these workers can move here.
"""
