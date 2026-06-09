{-# LANGUAGE StrictData #-}

module Control.Actor.Transport
  ( ConnHandle (..)
  , Transport (..)
  , createTCPTransport
  ) where

import Control.Actor.Types (NodeAddr (..))
import Control.Concurrent (forkIO)
import Control.Monad (forever, void, when)
import Data.Binary (decode, encode)
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Lazy (ByteString)
import Data.Word (Word64)
import Network.Socket
  ( AddrInfo (addrAddress, addrFamily, addrFlags, addrProtocol, addrSocketType)
  , AddrInfoFlag (AI_PASSIVE)
  , Socket
  , SocketOption (NoDelay, ReuseAddr)
  , SocketType (Stream)
  , accept
  , bind
  , close
  , connect
  , defaultHints
  , getAddrInfo
  , listen
  , setSocketOption
  , socket
  )
import Network.Socket.ByteString.Lazy (recv, sendAll)

data ConnHandle = ConnHandle
  { chSend  :: ByteString -> IO ()
  , chRecv  :: IO ByteString
  , chClose :: IO ()
  }

data Transport = Transport
  { tConnect :: NodeAddr -> IO ConnHandle
  , tListen  :: (ConnHandle -> IO ()) -> IO ()
  }

sendFramed :: Socket -> ByteString -> IO ()
sendFramed sock payload =
  sendAll sock (encode (fromIntegral (BL.length payload) :: Word64) <> payload)

recvExact :: Socket -> Int -> IO ByteString
recvExact sock = go []
  where
    go acc 0 = return (BL.concat (reverse acc))
    go acc r = do
      chunk <- recv sock (fromIntegral (min r 65536))
      when (BL.null chunk) $ fail "recvExact: connection closed"
      go (chunk : acc) (r - fromIntegral (BL.length chunk))

recvFramed :: Socket -> IO ByteString
recvFramed sock = do
  header <- recvExact sock 8
  recvExact sock (fromIntegral (decode header :: Word64))

connectTcp :: NodeAddr -> IO Socket
connectTcp (NodeAddr host port) = do
  let hints = defaultHints {addrSocketType = Stream}
  addrs <- getAddrInfo (Just hints) (Just host) (Just (show port))
  case addrs of
    []    -> fail $ "connectTcp: no address for " <> host <> ":" <> show port
    (a:_) -> do
      sock <- socket (addrFamily a) Stream (addrProtocol a)
      setSocketOption sock NoDelay 1
      connect sock (addrAddress a)
      return sock

listenTcp :: NodeAddr -> IO Socket
listenTcp (NodeAddr host port) = do
  let hints = defaultHints {addrFlags = [AI_PASSIVE], addrSocketType = Stream}
  addrs <- getAddrInfo (Just hints) (Just host) (Just (show port))
  case addrs of
    []    -> fail "listenTcp: no address"
    (a:_) -> do
      sock <- socket (addrFamily a) Stream (addrProtocol a)
      setSocketOption sock ReuseAddr 1
      bind sock (addrAddress a)
      listen sock 128
      return sock

createTCPTransport :: NodeAddr -> IO Transport
createTCPTransport myAddr = do
  lsock <- listenTcp myAddr
  return Transport
    { tConnect = \peer -> do
        sock <- connectTcp peer
        return ConnHandle
          { chSend  = sendFramed sock
          , chRecv  = recvFramed sock
          , chClose = close sock
          }
    , tListen = \callback -> void $ forkIO $ forever $ do
        (csock, _) <- accept lsock
        void $ forkIO $ callback ConnHandle
          { chSend  = sendFramed csock
          , chRecv  = recvFramed csock
          , chClose = close csock
          }
    }
