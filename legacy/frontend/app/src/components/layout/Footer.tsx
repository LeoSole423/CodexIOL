export function Footer() {
  return (
    <footer className="border-t border-border mt-auto">
      <div className="mx-auto max-w-screen-2xl px-4 sm:px-6 py-3">
        <p className="text-xs text-muted-foreground">
          Fuente:{" "}
          <code className="font-mono text-xs">data/iol_history.db</code>{" "}
          (snapshots locales)
        </p>
      </div>
    </footer>
  );
}
