#!/bin/sh

export NO_COLOR=true
/root/.deno/bin/deno serve -A --port $PORT dispatcher.ts
