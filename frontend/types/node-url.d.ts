declare class URL {
  constructor(url: string, base?: string | URL);
}

declare module 'node:url' {
  export function fileURLToPath(url: string | URL): string;
  export { URL };
}

interface ImportMeta {
  url: string;
}
