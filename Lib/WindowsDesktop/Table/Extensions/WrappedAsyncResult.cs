// -----------------------------------------------------------------------------------------
// <copyright file="DocumentHelpers.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
// -----------------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.Table.Extensions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Core.Util;

    /// <summary>
    /// This class is the same as CancellableAsyncResultTaskWrapper, except it's used to wrap operations that return a Task&lt;TResult&gt; (instead of just a Task).
    /// TODO: This should be combined with CancellableAsyncResultTaskWrapper<> by extending it to support executor model
    /// </summary>
    /// <typeparam name="TResult">The return type of the operation to wrap</typeparam>
    /// <typeparam name="TExecutor">The type of executor depending on the backend</typeparam>
    internal class WrappedAsyncResult<TResult, TExecutor> : CancellableAsyncResultTaskWrapper
    {
        /// <summary>
        /// Creates a new ICancellableAsyncResult Task&lt;TResult&gt; wrapper object.
        /// </summary>
        /// <param name="generateTask">This is essentially the async method that does the actual work we want to wrap.</param>
        /// <param name="callback">An <see cref="AsyncCallback"/> delegate that will receive notification when the asynchronous operation completes.</param>
        /// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        public WrappedAsyncResult(
            Func<CancellationToken, Task<TResult>> generateTask, 
            TExecutor executor, 
            AsyncCallback callback, 
            Object state) 
            : base()
        {
            this.Executor = executor;

            // We cannot pass the user callback into the AsApm method, because it breaks the general APM contract - namely, that the IAsyncResult returned from the Begin method
            // is what's passed into the callback. The AsApm method will pass in this.internalAsyncResult to its callback, not this.
            AsyncCallback newCallback = ar =>
            {
                // Avoid the potential race condition where the callback is called before AsApm returns.
                this.internalAsyncResult = ar;
                callback(this);
            };

            this.internalAsyncResult = generateTask(cancellationTokenSource.Token).AsApm(newCallback, state);
        }

        internal TExecutor Executor { get; private set; }

        internal TResult Result
        {
            get { return ((Task<TResult>) this.internalAsyncResult).Result; }
        }
    }
}
