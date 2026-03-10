import type { ReactNode } from "react";

interface PageContainerProps {
  title: string;
  children: ReactNode;
}

export function PageContainer({ title, children }: PageContainerProps) {
  return (
    <div className="px-6 py-6">
      <h1 className="mb-6 text-2xl font-semibold text-foreground">{title}</h1>
      {children}
    </div>
  );
}
