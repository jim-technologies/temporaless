import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

const files = [
  [
    "core/ts/src/gen/temporaless/v1/temporaless_descriptor.binpb",
    "core/ts/dist/gen/temporaless/v1/temporaless_descriptor.binpb",
  ],
];

for (const [src, dst] of files) {
  const target = resolve(dst);
  mkdirSync(dirname(target), { recursive: true });
  copyFileSync(resolve(src), target);
}
