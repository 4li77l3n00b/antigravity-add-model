/** Only the trusted local desktop top frame can launch an SSH ACP controller. */
import { app, ipcMain } from 'electron';
import * as path from 'node:path';
import { RemoteAcpManager } from './remoteAcp';
import { NativeRemoteService } from './nativeRemote';

export function trustedRemoteAcpFrame(event: any, port: number): boolean {
  try {
    const url = new URL(event.senderFrame.url);
    return (
      port > 0 &&
      event.senderFrame === event.sender.mainFrame &&
      ['http:', 'https:'].includes(url.protocol) &&
      ['localhost', '127.0.0.1'].includes(url.hostname) &&
      Number(url.port) === port &&
      !url.username &&
      !url.password
    );
  } catch {
    return false;
  }
}
export function registerRemoteAcp(getLanguageServerPort: () => number): void {
  const manager = new RemoteAcpManager(path.join(app.getPath('home'), '.gemini', 'antigravity'));
  const native = new NativeRemoteService(manager);
  const owners = new Set<number>();
  for (const channel of ['remote-acp:native', 'remote-acp:native-snapshot', 'remote-acp:native-catalog']) {
    ipcMain.handle(channel, async (event: any, method?: string, input?: Record<string, unknown>) => {
      if (!trustedRemoteAcpFrame(event, getLanguageServerPort()))
        return { success: false, error: 'SSH ACP requires the trusted desktop main frame.' };
      try {
        if (channel === 'remote-acp:native-catalog') {
          native.captureCatalog(input || {});
          return { success: true };
        }
        return {
          success: true,
          result:
            channel === 'remote-acp:native-snapshot' ? await native.snapshot() : await native.call(method!, input),
        };
      } catch (error) {
        return { success: false, error: (error as Error).message };
      }
    });
  }
  ipcMain.handle('remote-acp:action', async (event: any, operation: string, input?: Record<string, unknown>) => {
    if (!trustedRemoteAcpFrame(event, getLanguageServerPort()))
      return { success: false, error: 'SSH ACP requires the trusted desktop main frame.' };
    const owner = event.sender.id;
    if (!owners.has(owner)) {
      owners.add(owner);
      event.sender.once('destroyed', () => {
        owners.delete(owner);
        manager.release(owner);
      });
    }
    try {
      return { success: true, result: await manager.action(owner, operation, input) };
    } catch (error) {
      return { success: false, error: (error as Error).message };
    }
  });
  app.on('will-quit', () => {
    native.close();
    manager.close();
  });
}
