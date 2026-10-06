# Sincronizador de pastas P2P com UDP

Trabalho criado por **Luca Pires Soares**.

Trabalho I da disciplina de Redes de Computadores: Aplicação e Transporte (UNISINOS).

O programa mantém uma pasta igual em vários computadores ao mesmo tempo. Se você coloca um arquivo na pasta de um peer, ele aparece nos outros em poucos segundos. Se apaga, ele some de todos. Não existe servidor central: cada peer roda o mesmo programa e faz as duas funções, servidor (envia arquivos para quem pede) e cliente (avisa os outros das mudanças e baixa o que está faltando). Toda a comunicação é feita por UDP.

## O que precisa

- Python 3.8 ou mais novo (`python --version` para conferir)
- Nenhuma biblioteca extra, só a biblioteca padrão do Python

## Arquivos do projeto

| Arquivo | Para que serve |
|---|---|
| `peer.py` | O programa. É o mesmo para todos os peers |
| `peers.local.txt` | Lista de peers para testar tudo no mesmo computador |
| `peers.txt` | Lista de peers para rodar em computadores diferentes (editar os IPs) |
| `peers.docker.txt`, `Dockerfile`, `docker-compose.yml` | Para rodar os peers em containers Docker (opcional) |

Os arquivos de configuração têm uma linha por peer, no formato `ID HOST PORTA`:

```
A 127.0.0.1 5001
B 127.0.0.1 5002
C 127.0.0.1 5003
D 127.0.0.1 5004
```

Não tem descoberta automática de peers, então todo mundo precisa estar nessa lista. Para colocar mais um peer na rede é só acrescentar uma linha.

## Testando no mesmo computador

Abra quatro terminais na pasta do projeto. No Windows, uma forma rápida é clicar na barra de endereço do Explorador, digitar `cmd` e apertar Enter.

Rode um comando em cada terminal:

```
Terminal 1: python peer.py A --config peers.local.txt --dir tmpA --status 5
Terminal 2: python peer.py B --config peers.local.txt --dir tmpB --status 5
Terminal 3: python peer.py C --config peers.local.txt --dir tmpC --status 5
Terminal 4: python peer.py D --config peers.local.txt --dir tmpD --status 5
```

As pastas `tmpA`, `tmpB`, `tmpC` e `tmpD` são criadas sozinhas na primeira execução. Se o Windows perguntar sobre o firewall, clique em Permitir.

Dá para começar só com A, B e C e deixar o D para depois. Assim se vê um peer novo entrando com a pasta vazia e recebendo tudo que já existia na rede.

Para testar:

1. Copie um arquivo qualquer para `tmpA`. Ele deve aparecer em `tmpB`, `tmpC` (e `tmpD`, se estiver rodando).
2. Apague o arquivo em `tmpB`. Ele some das outras pastas.
3. Inicie o peer D com a pasta vazia e veja ele baixar todos os arquivos.

**Dica:** crie e dê nome aos arquivos fora das pastas sincronizadas (na Área de Trabalho, por exemplo) e só depois copie para dentro. Se você usar "Novo > Documento de Texto" direto na pasta, o arquivo é enviado para os outros peers com o nome provisório enquanto você ainda está digitando o nome. Funciona, mas o Explorador do Windows costuma se perder com isso.

## Rodando em computadores diferentes

1. Copie `peer.py` e `peers.txt` para cada computador.
2. Descubra o IP de cada um (`ipconfig` no Windows, `ip a` no Linux).
3. Edite `peers.txt` com esses IPs. O arquivo tem que ser igual em todas as máquinas:
   ```
   A 192.168.0.10 5000
   B 192.168.0.11 5000
   C 192.168.0.12 5000
   ```
4. Libere a porta UDP 5000 no firewall.
5. Em cada computador rode o comando com o ID daquela máquina:
   ```
   python peer.py A --status 5
   ```
   (`B` no segundo, `C` no terceiro. A pasta usada é `tmp`.)

Funciona também com máquinas virtuais, desde que estejam em modo de rede "bridge" e consigam se enxergar na rede.

### Com Docker

```
docker compose up -d --build
```

Isso sobe os peers A, B e C, cada um em um container. As pastas sincronizadas ficam em `data/a`, `data/b` e `data/c` no seu computador. Para subir o quarto peer:

```
docker compose --profile novo up -d peer-d
```

Para ver o terminal de um peer: `docker attach peer-a`. Para sair sem derrubar o container, aperte `Ctrl+P` e depois `Ctrl+Q`.

## Opções e comandos

| Opção | O que faz |
|---|---|
| `--config arquivo` | Arquivo com a lista de peers (padrão: `peers.txt`) |
| `--dir pasta` | Pasta que será sincronizada (padrão: `tmp`) |
| `--status N` | Mostra o estado da rede a cada N segundos |
| `--perda 0.3` | Descarta de propósito 30% dos pacotes enviados, para testar a recuperação de perdas |

Com o peer rodando, dá para digitar no terminal:

- `status`: mostra a tabela com cada peer, se está ativo e quais arquivos tem
- `lista`: mostra os arquivos da rede com tamanho e hash
- `sync`: força uma sincronização com todos
- `sair`: encerra o peer (`Ctrl+C` também funciona)

Exemplo da tela de status:

```
======================================================================
 ESTADO DA REDE - visto pelo peer A  (21:40:12)
======================================================================
 PEER      STATUS    ULT.MSG  QTD  ARQUIVOS
----------------------------------------------------------------------
 A (eu)    ATIVO     -        2    notas.txt, relatorio.pdf
 B         ATIVO     1s       2    notas.txt, relatorio.pdf
 C         ATIVO     0s       2    notas.txt, relatorio.pdf
 D         inativo   nunca    -    -
----------------------------------------------------------------------
 Rede SINCRONIZADA
======================================================================
```

## Como funciona

### Mensagens

As mensagens são texto em JSON, uma por datagrama UDP:

| Mensagem | Quando é enviada |
|---|---|
| `ANUNCIO` | Apareceu ou mudou um arquivo na minha pasta (nome, tamanho, hash, horário) |
| `REMOVIDO` | Um arquivo foi apagado da minha pasta |
| `LISTA` | Pedido da lista completa de arquivos de outro peer |
| `LISTA_RESP` | Resposta com a lista (dividida em várias mensagens se for grande) |
| `PEDIR` | Pedido de pedaços de um arquivo |
| `DADOS` | Um pedaço de 4 KB do arquivo, em base64 |
| `PING` | Enviado a cada 2 segundos para dizer "estou vivo" e quantos arquivos tenho |
| `ACK` | Confirmação de que um `ANUNCIO` ou `REMOVIDO` chegou |

### O que acontece quando um arquivo é adicionado

1. A cada segundo o peer olha a própria pasta. Quando encontra um arquivo novo, calcula o hash SHA-256 dele e manda um `ANUNCIO` para todos.
2. Quem recebe e ainda não tem aquela versão manda um `PEDIR` com a lista de pedaços que quer (até 256 por vez).
3. O dono responde com várias mensagens `DADOS`, uma por pedaço.
4. Quando chegam todos os pedaços, o arquivo é montado, o hash é conferido e só então ele é gravado na pasta. A gravação é feita primeiro num arquivo temporário oculto (`.nome.part`) e depois renomeada, para nunca aparecer um arquivo pela metade.

Remoção é parecida: quem apagou manda `REMOVIDO` e os outros apagam a sua cópia. O peer guarda um registro de que o arquivo foi removido. Assim, se alguém estava desligado e volta com a cópia antiga, ela é apagada em vez de ser espalhada de novo.

### Lidando com o UDP

O UDP não garante que a mensagem chegue, nem a ordem, nem que chegue uma vez só. Por isso o próprio programa trata isso:

- **Perda:** `ANUNCIO` e `REMOVIDO` são reenviados a cada segundo até chegar o `ACK` de cada peer. Pedaços de arquivo que não chegam em 0,4 s são pedidos de novo. Se um peer parar de responder no meio de um download, o programa tenta baixar de outro que tenha o mesmo arquivo.
- **Duplicação:** cada mensagem importante tem um identificador. Se chegar repetida, é ignorada. Pedaços repetidos também são descartados.
- **Ordem:** cada pedaço tem um número, então a ordem de chegada não importa.
- **Segurança extra:** a cada 5 segundos cada peer pede a `LISTA` dos outros e compara com a sua. Se alguma mensagem se perdeu de vez, a diferença aparece aqui e é corrigida.

Para ver isso funcionando, rode os peers com `--perda 0.3`. Mesmo com 30% dos pacotes jogados fora, as pastas terminam iguais. Só demora um pouco mais.

### Peers entrando e saindo

Um peer é considerado ativo enquanto manda `PING`. Se ficar 6 segundos sem dar sinal, aparece como inativo no status. Quando um peer aparece (ou volta), os outros pedem a `LISTA` dele e ele pede a dos outros. É por isso que um peer novo, com a pasta vazia, baixa tudo sozinho logo que entra.

Se chegar mensagem de um peer que não está no arquivo de configuração, ele também passa a fazer parte da rede.

### Conflitos

Se o mesmo arquivo for alterado em dois peers quase ao mesmo tempo, fica valendo a versão com horário mais recente.

## Limitações

- Só sincroniza arquivos que estão direto na pasta, sem subpastas.
- Arquivos que começam com ponto (ocultos) são ignorados.
- Os conflitos dependem do relógio das máquinas. Se os relógios estiverem muito diferentes, a versão "mais nova" pode não ser a de verdade.
- Durante o download o arquivo fica inteiro na memória, então o programa foi pensado para arquivos de até algumas dezenas de MB.
- Não tem autenticação: qualquer um na rede que conheça o formato das mensagens consegue participar.

## Problemas comuns

**`can't open file 'peer.py'`:** o terminal não está na pasta do projeto. Use `cd` até a pasta onde está o `peer.py` (confira com `dir` ou `ls`).

**Os peers não se veem em computadores diferentes:** quase sempre é firewall. Libere a porta UDP 5000 e confira se os IPs do `peers.txt` estão certos e se as máquinas estão na mesma rede.

**`Address already in use` / erro de porta:** já tem um peer rodando com aquela porta. Feche o outro terminal ou use outro ID.

**O Explorador do Windows trava:** evite criar e renomear arquivos direto nas pastas sincronizadas (veja a dica na seção de teste).
