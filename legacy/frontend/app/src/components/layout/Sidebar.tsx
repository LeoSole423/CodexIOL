"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Moon, Sun, BarChart3,
  LayoutDashboard, Brain, Cpu, FlaskConical,
  ShieldCheck, Wallet, Clock,
} from "lucide-react";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";

const NAV_LINKS = [
  { href: "/",          label: "Dashboard",   icon: LayoutDashboard },
  { href: "/advisor",   label: "Asesor",      icon: Brain           },
  { href: "/engines",   label: "Motores",     icon: Cpu             },
  { href: "/simulate",  label: "Simulación",  icon: FlaskConical    },
  { href: "/quality",   label: "Calidad",     icon: ShieldCheck     },
  { href: "/assets",    label: "Activos",     icon: Wallet          },
  { href: "/history",   label: "Historia",    icon: Clock           },
] as const;

function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => setMounted(true), []);
  if (!mounted) return <div className="w-9 h-9" />;

  return (
    <button
      onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
      className={cn(
        "flex items-center gap-3 w-full px-3 py-2 rounded-md",
        "text-muted-foreground hover:text-foreground hover:bg-accent",
        "transition-colors duration-150 text-sm"
      )}
      aria-label="Toggle theme"
    >
      {theme === "dark" ? (
        <Sun className="h-4 w-4 shrink-0" />
      ) : (
        <Moon className="h-4 w-4 shrink-0" />
      )}
      <span>{theme === "dark" ? "Claro" : "Oscuro"}</span>
    </button>
  );
}

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="sticky top-0 h-screen w-52 shrink-0 flex flex-col border-r border-border bg-surface">
      {/* Brand */}
      <div className="px-4 py-5 border-b border-border">
        <Link href="/" className="flex items-center gap-2.5">
          <div className="flex items-center justify-center w-8 h-8 rounded-md bg-primary shrink-0">
            <BarChart3 className="h-4 w-4 text-primary-foreground" />
          </div>
          <div>
            <div className="text-sm font-semibold text-foreground leading-none">
              IOL Portfolio
            </div>
            <div className="text-xs text-muted-foreground leading-none mt-0.5">
              Dashboard
            </div>
          </div>
        </Link>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 flex flex-col gap-0.5">
        {NAV_LINKS.map(({ href, label, icon: Icon }) => {
          const isActive =
            href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium",
                "transition-colors duration-150",
                isActive
                  ? "bg-accent text-foreground"
                  : "text-muted-foreground hover:text-foreground hover:bg-accent"
              )}
            >
              <Icon className="h-4 w-4 shrink-0" />
              {label}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-3 py-4 border-t border-border">
        <ThemeToggle />
      </div>
    </aside>
  );
}
