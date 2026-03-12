import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Search } from "lucide-react";
import { usePlayerSearch } from "@/api/queries/players";
import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Badge } from "@/components/ui/badge";

export function PlayerSearch() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const navigate = useNavigate();

  const debouncedQuery = useDebounce(query, 300);
  const { data: results, isLoading } = usePlayerSearch(debouncedQuery);

  // Cmd+K / Ctrl+K keyboard shortcut
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((prev) => !prev);
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

  function handleSelect(playerId: number) {
    setOpen(false);
    setQuery("");
    navigate(`/roster/${playerId}`);
  }

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="flex w-full items-center gap-2 rounded-lg border border-input bg-transparent px-3 py-1.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
      >
        <Search className="size-4" />
        <span className="flex-1 text-left">Search players...</span>
        <kbd className="pointer-events-none hidden h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium opacity-100 sm:flex">
          <span className="text-xs">&#8984;</span>K
        </kbd>
      </button>

      <CommandDialog
        open={open}
        onOpenChange={setOpen}
        title="Player Search"
        description="Search for a player by name"
      >
        <Command shouldFilter={false}>
          <CommandInput
            placeholder="Search players..."
            value={query}
            onValueChange={setQuery}
          />
          <CommandList>
            {debouncedQuery.length < 2 ? (
              <CommandEmpty>Type at least 2 characters to search</CommandEmpty>
            ) : isLoading ? (
              <CommandEmpty>Searching...</CommandEmpty>
            ) : !results?.length ? (
              <CommandEmpty>No players found</CommandEmpty>
            ) : (
              <CommandGroup heading="Players">
                {results.map((player) => (
                  <CommandItem
                    key={player.id}
                    value={String(player.id)}
                    onSelect={() => handleSelect(player.id)}
                  >
                    <span className="font-medium">{player.name}</span>
                    <Badge variant="secondary" className="ml-auto">
                      {player.position}
                    </Badge>
                    {player.nfl_team && (
                      <span className="text-xs text-muted-foreground">
                        {player.nfl_team}
                      </span>
                    )}
                  </CommandItem>
                ))}
              </CommandGroup>
            )}
          </CommandList>
        </Command>
      </CommandDialog>
    </>
  );
}

function useDebounce(value: string, delay: number): string {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);

  return debounced;
}
